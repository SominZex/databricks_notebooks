# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# Databricks Notebook: 11_silver_stock

# ============================================================
# 11_silver_stock
#
# Bronze:
#   catalog.schema.stock_bronze
#
# Silver:
#   catalog.schema.stock_silver
#
# Purpose:
#   - Clean stock API data
#   - Standardize column names
#   - Normalize metadata columns
#   - Cast known fields
#   - Validate snapshot_date
#   - Remove duplicate stock rows
#   - Preserve API columns
#   - Idempotent TARGET_DATE processing
# ============================================================


# ============================================================
# Imports
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime


# ============================================================
# Configuration
# ============================================================

BRONZE_TABLE = (
    f"{catalog_name}.{schema_name}.stock_bronze"
)

SILVER_TABLE = (
    f"{catalog_name}.{schema_name}.stock_silver"
)

RUN_DATE = datetime.strptime(
    str(TARGET_DATE),
    "%Y-%m-%d"
).date()


print("=" * 70)
print("SILVER STOCK TRANSFORMATION")
print("=" * 70)

print(f"Target date : {RUN_DATE}")
print(f"Bronze      : {BRONZE_TABLE}")
print(f"Silver      : {SILVER_TABLE}")

print("=" * 70)


# ============================================================
# Helper functions
# ============================================================

def normalize_column_name(column_name):
    """
    Convert API column names into Spark-safe snake_case names.
    """

    import re

    name = str(column_name).strip()

    # Handle camelCase
    name = re.sub(
        r"(.)([A-Z][a-z]+)",
        r"\1_\2",
        name
    )

    name = re.sub(
        r"([a-z0-9])([A-Z])",
        r"\1_\2",
        name
    )

    # Replace non-alphanumeric characters
    name = re.sub(
        r"[^a-zA-Z0-9]+",
        "_",
        name
    )

    # Lowercase
    name = name.lower()

    # Remove duplicate underscores
    name = re.sub(
        r"_+",
        "_",
        name
    ).strip("_")

    return name


def unique_column_names(columns):
    """
    Normalize column names while guaranteeing uniqueness.
    """

    result = []
    used = {}

    for original in columns:

        normalized = normalize_column_name(original)

        if not normalized:
            normalized = "unnamed_column"

        if normalized not in used:

            used[normalized] = 0
            result.append(normalized)

        else:

            used[normalized] += 1

            candidate = (
                f"{normalized}_{used[normalized]}"
            )

            while candidate in used:

                used[normalized] += 1

                candidate = (
                    f"{normalized}_{used[normalized]}"
                )

            used[candidate] = 0
            result.append(candidate)

    return result


def find_column(columns, candidates):
    """
    Find the first matching column from candidate names.
    """

    column_set = set(columns)

    for candidate in candidates:

        if candidate in column_set:
            return candidate

    return None


# ============================================================
# Check Bronze exists
# ============================================================

if not spark.catalog.tableExists(BRONZE_TABLE):

    raise RuntimeError(
        f"Bronze table does not exist: {BRONZE_TABLE}"
    )


# ============================================================
# Read Bronze
# ============================================================

bronze_df = spark.table(BRONZE_TABLE)


# ============================================================
# Inspect Bronze schema
# ============================================================

print("=" * 70)
print("BRONZE SCHEMA")
print("=" * 70)

for column_name in bronze_df.columns:
    print(f"  - {column_name}")

print("=" * 70)


# ============================================================
# Resolve metadata columns
#
# Support BOTH:
#
#   _store_id       / store_id
#   _store_name     / store_name
#   _snapshot_date  / snapshot_date
#   _run_id         / run_id
#   _ingested_at    / ingested_at
#
# This makes the notebook backward-compatible with the
# current Bronze table.
# ============================================================

store_id_source = find_column(
    bronze_df.columns,
    [
        "_store_id",
        "store_id"
    ]
)

store_name_source = find_column(
    bronze_df.columns,
    [
        "_store_name",
        "store_name"
    ]
)

snapshot_date_source = find_column(
    bronze_df.columns,
    [
        "_snapshot_date",
        "snapshot_date"
    ]
)

run_id_source = find_column(
    bronze_df.columns,
    [
        "_run_id",
        "run_id"
    ]
)

ingested_at_source = find_column(
    bronze_df.columns,
    [
        "_ingested_at",
        "ingested_at"
    ]
)


print("=" * 70)
print("METADATA COLUMN RESOLUTION")
print("=" * 70)

print(f"Store ID       : {store_id_source}")
print(f"Store Name     : {store_name_source}")
print(f"Snapshot Date  : {snapshot_date_source}")
print(f"Run ID         : {run_id_source}")
print(f"Ingested At    : {ingested_at_source}")

print("=" * 70)


# ============================================================
# Validate required metadata
# ============================================================

missing_metadata = []

if store_id_source is None:
    missing_metadata.append("store_id")

if store_name_source is None:
    missing_metadata.append("store_name")

if snapshot_date_source is None:
    missing_metadata.append("snapshot_date")


if missing_metadata:

    raise RuntimeError(
        "Bronze table is missing required metadata columns: "
        f"{missing_metadata}"
    )


# ============================================================
# Read TARGET_DATE
# ============================================================

df = (
    bronze_df
    .filter(
        F.to_date(
            F.col(snapshot_date_source)
        ) == F.lit(RUN_DATE)
    )
)


# ============================================================
# Initial validation
# ============================================================

bronze_rows = df.count()

if bronze_rows == 0:

    raise RuntimeError(
        f"No Bronze stock data found for "
        f"snapshot date {RUN_DATE}"
    )


bronze_stores = (
    df
    .select(
        F.col(store_id_source).alias("_store_id")
    )
    .where(
        F.col("_store_id").isNotNull()
    )
    .distinct()
    .count()
)


print("=" * 70)
print("BRONZE INPUT")
print("=" * 70)

print(f"Rows   : {bronze_rows:,}")
print(f"Stores : {bronze_stores:,}")

print("=" * 70)


# ============================================================
# Normalize API column names
#
# IMPORTANT:
# Metadata is handled separately.
# We explicitly create canonical internal metadata names:
#
#   _store_id
#   _store_name
#   _snapshot_date
#   _run_id
#   _ingested_at
#
# This prevents the previous unresolved-column problem.
# ============================================================

metadata_source_columns = {
    store_id_source,
    store_name_source,
    snapshot_date_source,
    run_id_source,
    ingested_at_source,
}

# Normalize all non-metadata columns
non_metadata_columns = [
    c
    for c in df.columns
    if c not in metadata_source_columns
]

normalized_non_metadata = unique_column_names(
    non_metadata_columns
)


# ============================================================
# Build normalized DataFrame
# ============================================================

select_expressions = []

# ------------------------------------------------------------
# API columns
# ------------------------------------------------------------

for old_name, new_name in zip(
    non_metadata_columns,
    normalized_non_metadata
):

    select_expressions.append(
        F.col(old_name).alias(new_name)
    )


# ------------------------------------------------------------
# Canonical metadata
# ------------------------------------------------------------

select_expressions.append(
    F.trim(
        F.col(store_id_source).cast("string")
    ).alias("_store_id")
)

select_expressions.append(
    F.trim(
        F.col(store_name_source).cast("string")
    ).alias("_store_name")
)

select_expressions.append(
    F.to_date(
        F.col(snapshot_date_source)
    ).alias("_snapshot_date")
)


# ------------------------------------------------------------
# Optional metadata
# ------------------------------------------------------------

if run_id_source:

    select_expressions.append(
        F.col(run_id_source)
        .cast("string")
        .alias("_run_id")
    )

else:

    select_expressions.append(
        F.lit(None)
        .cast("string")
        .alias("_run_id")
    )


if ingested_at_source:

    select_expressions.append(
        F.to_timestamp(
            F.col(ingested_at_source)
        ).alias("_ingested_at")
    )

else:

    select_expressions.append(
        F.lit(None)
        .cast("timestamp")
        .alias("_ingested_at")
    )


df = df.select(*select_expressions)


# ============================================================
# Resolve duplicate store-name issue
# ============================================================
#
# The Bronze input shown in the failed run contains:
#
#   store_name
#   store_name_1
#
# Normally store_name is the API field.
#
# Because metadata is now explicitly stored as _store_name,
# both API columns can safely coexist.
#
# However, if store_name_1 is simply a duplicate copy of
# store_name, remove it.
# ============================================================

if (
    "store_name" in df.columns
    and "store_name_1" in df.columns
):

    duplicate_store_name_check = (
        df
        .filter(
            ~(
                F.coalesce(
                    F.col("store_name"),
                    F.lit("")
                )
                ==
                F.coalesce(
                    F.col("store_name_1"),
                    F.lit("")
                )
            )
        )
        .limit(1)
        .count()
    )

    if duplicate_store_name_check == 0:

        print(
            "Removing duplicate API column: store_name_1"
        )

        df = df.drop("store_name_1")

    else:

        print(
            "WARNING: store_name and store_name_1 "
            "contain different values. "
            "Keeping both columns."
        )


# ============================================================
# Re-read column list
# ============================================================

columns = df.columns

print("=" * 70)
print("SILVER COLUMNS AFTER NORMALIZATION")
print("=" * 70)

for column_name in columns:

    print(f"  - {column_name}")

print("=" * 70)


# ============================================================
# Normalize internal metadata
# ============================================================

df = (
    df
    .withColumn(
        "_store_id",
        F.trim(
            F.col("_store_id").cast("string")
        )
    )
    .withColumn(
        "_store_name",
        F.trim(
            F.col("_store_name").cast("string")
        )
    )
    .withColumn(
        "_snapshot_date",
        F.to_date(
            F.col("_snapshot_date")
        )
    )
)


# ============================================================
# Cast ingestion timestamp
# ============================================================

df = df.withColumn(
    "_ingested_at",
    F.to_timestamp("_ingested_at")
)


# ============================================================
# Cast common stock fields
# ============================================================

numeric_columns = {

    # Quantity
    "quantity",
    "qty",
    "stock_quantity",
    "available_quantity",
    "available_qty",
    "closing_stock",
    "opening_stock",

    # Pricing
    "selling_price",
    "cost_price",
    "printed_mrp",
    "mrp",
    "price",

    # Value
    "total_amount",
    "total_value",
    "stock_value",
    "amount",
}


integer_columns = {

    "quantity",
    "qty",
    "stock_quantity",
    "available_quantity",
    "available_qty",
    "closing_stock",
    "opening_stock",
}


for column_name in df.columns:

    if column_name in numeric_columns:

        cleaned_value = (
            F.regexp_replace(
                F.trim(
                    F.col(column_name).cast("string")
                ),
                ",",
                ""
            )
        )

        # Keep quantity as double, matching the existing
        # stock Bronze/Silver behavior.
        df = df.withColumn(
            column_name,
            cleaned_value.cast("double")
        )


# ============================================================
# Clean string columns
# ============================================================

string_columns = [
    field.name
    for field in df.schema.fields
    if field.dataType.typeName() == "string"
]


for column_name in string_columns:

    if column_name in {
        "_store_id",
        "_store_name",
        "_run_id",
    }:
        continue

    df = df.withColumn(
        column_name,
        F.trim(
            F.col(column_name)
        )
    )


# ============================================================
# Validate snapshot date
# ============================================================

wrong_date_count = (
    df
    .filter(
        F.col("_snapshot_date")
        != F.lit(RUN_DATE)
    )
    .count()
)


if wrong_date_count > 0:

    raise RuntimeError(
        f"Found {wrong_date_count:,} rows with "
        f"snapshot_date different from TARGET_DATE "
        f"{RUN_DATE}."
    )


# ============================================================
# Validate store metadata
# ============================================================

missing_store_id = (
    df
    .filter(
        F.col("_store_id").isNull()
        |
        (
            F.trim(
                F.col("_store_id")
            ) == ""
        )
    )
    .count()
)


if missing_store_id > 0:

    raise RuntimeError(
        f"Found {missing_store_id:,} stock rows "
        "without store ID."
    )


# ============================================================
# Detect likely business keys
# ============================================================

barcode_col = find_column(
    df.columns,
    [
        "barcode",
        "bar_code",
        "sku",
        "sku_code",
    ]
)


product_id_col = find_column(
    df.columns,
    [
        "product_id",
        "productid",
        "product_code",
    ]
)


product_name_col = find_column(
    df.columns,
    [
        "product_name",
        "product",
    ]
)


print("=" * 70)
print("KEY DETECTION")
print("=" * 70)

print(f"Barcode column    : {barcode_col}")
print(f"Product ID column : {product_id_col}")
print(f"Product name      : {product_name_col}")

print("=" * 70)


# ============================================================
# Duplicate handling
# ============================================================
#
# Preferred identity:
#
#   store + product_id
#
# otherwise:
#
#   store + barcode
#
# otherwise:
#
#   store + product_name
#
# Snapshot date is always part of the logical record.
#
# Latest ingested record is retained.
# ============================================================

dedupe_columns = [
    "_store_id",
    "_snapshot_date",
]


if product_id_col:

    dedupe_columns.append(
        product_id_col
    )

elif barcode_col:

    dedupe_columns.append(
        barcode_col
    )

elif product_name_col:

    dedupe_columns.append(
        product_name_col
    )

else:

    print(
        "WARNING: No product identifier found. "
        "Row-level duplicate removal will be used."
    )


# ============================================================
# Remove rows with missing primary identity
# ============================================================

identity_col = None

if product_id_col:

    identity_col = product_id_col

elif barcode_col:

    identity_col = barcode_col

elif product_name_col:

    identity_col = product_name_col


if identity_col:

    before_null_identity = df.count()

    df = df.filter(
        F.col(identity_col).isNotNull()
        &
        (
            F.trim(
                F.col(identity_col).cast("string")
            ) != ""
        )
    )

    after_null_identity = df.count()

    removed_null_identity = (
        before_null_identity
        - after_null_identity
    )

    if removed_null_identity:

        print(
            f"Removed {removed_null_identity:,} rows "
            f"with missing {identity_col}."
        )


# ============================================================
# Deduplicate
# ============================================================

before_dedupe = df.count()


window = (
    Window
    .partitionBy(*dedupe_columns)
    .orderBy(
        F.col("_ingested_at").desc_nulls_last()
    )
)


df = (
    df
    .withColumn(
        "_row_number",
        F.row_number().over(window)
    )
    .filter(
        F.col("_row_number") == 1
    )
    .drop("_row_number")
)


after_dedupe = df.count()

duplicate_rows_removed = (
    before_dedupe
    - after_dedupe
)


print("=" * 70)
print("DEDUPLICATION")
print("=" * 70)

print(f"Before : {before_dedupe:,}")
print(f"After  : {after_dedupe:,}")
print(f"Removed: {duplicate_rows_removed:,}")

print("=" * 70)


# ============================================================
# Add processing metadata
# ============================================================

df = df.withColumn(
    "_processed_date",
    F.current_date()
)


# ============================================================
# Final validation
# ============================================================

final_rows = df.count()

final_stores = (
    df
    .select("_store_id")
    .where(
        F.col("_store_id").isNotNull()
    )
    .distinct()
    .count()
)


if final_rows == 0:

    raise RuntimeError(
        "Silver stock DataFrame contains zero rows."
    )


if final_stores == 0:

    raise RuntimeError(
        "Silver stock DataFrame contains zero stores."
    )


print("=" * 70)
print("FINAL SILVER DATASET")
print("=" * 70)

print(f"Rows   : {final_rows:,}")
print(f"Stores : {final_stores:,}")
print(f"Date   : {RUN_DATE}")

print("=" * 70)


# ============================================================
# Create Silver table if required
# ============================================================

if not spark.catalog.tableExists(SILVER_TABLE):

    print(
        f"Creating Silver table: {SILVER_TABLE}"
    )

    (
        df
        .limit(0)
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(SILVER_TABLE)
    )


# ============================================================
# Schema evolution
# ============================================================

existing_schema = (
    spark.table(SILVER_TABLE).schema
)

existing_columns = [
    field.name
    for field in existing_schema.fields
]


incoming_columns = df.columns


new_columns = [
    c
    for c in incoming_columns
    if c not in existing_columns
]


if new_columns:

    print(
        "Adding new Silver columns:",
        new_columns
    )

    for column_name in new_columns:

        field = next(
            field
            for field in df.schema.fields
            if field.name == column_name
        )

        spark.sql(
            f"""
            ALTER TABLE {SILVER_TABLE}
            ADD COLUMNS (
                `{column_name}`
                {field.dataType.simpleString()}
            )
            """
        )


# ============================================================
# Align DataFrame to Silver schema
# ============================================================

silver_schema = (
    spark.table(SILVER_TABLE).schema
)


for field in silver_schema.fields:

    if field.name not in df.columns:

        df = df.withColumn(
            field.name,
            F.lit(None).cast(
                field.dataType
            )
        )


df = df.select(
    *[
        field.name
        for field in silver_schema.fields
    ]
)


# ============================================================
# Idempotent TARGET_DATE replacement
# ============================================================
#
# Only the requested snapshot is replaced.
# Historical snapshots remain untouched.
# ============================================================

spark.sql(
    f"""
    DELETE FROM {SILVER_TABLE}
    WHERE _snapshot_date = DATE('{RUN_DATE.isoformat()}')
    """
)


# ============================================================
# Write Silver
# ============================================================

(
    df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(SILVER_TABLE)
)


# ============================================================
# Post-write validation
# ============================================================

validation_df = (
    spark
    .table(SILVER_TABLE)
    .filter(
        F.col("_snapshot_date")
        == F.lit(RUN_DATE)
    )
)


written_rows = validation_df.count()


written_stores = (
    validation_df
    .select("_store_id")
    .distinct()
    .count()
)


if written_rows == 0:

    raise RuntimeError(
        "Post-write validation failed: "
        "no Silver rows found."
    )


if written_stores == 0:

    raise RuntimeError(
        "Post-write validation failed: "
        "no Silver stores found."
    )


# ============================================================
# Completion
# ============================================================

print()

print("=" * 70)
print("SILVER STOCK COMPLETED SUCCESSFULLY")
print("=" * 70)

print(f"Snapshot date       : {RUN_DATE}")
print(f"Rows written        : {written_rows:,}")
print(f"Stores              : {written_stores:,}")
print(f"Duplicates removed  : {duplicate_rows_removed:,}")
print(f"Silver table        : {SILVER_TABLE}")

print("=" * 70)

# COMMAND ----------

