# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------


# =============================================================================
# 09_silver_rtv
#
# RTV BRONZE -> SILVER
#
# Features:
#   - Incremental batch processing
#   - Idempotent replacement of the selected Bronze batch
#   - Uses the existing Silver schema
#   - Automatically aligns incoming data types with Silver data types
#   - Does not require metadata columns absent from Silver
#   - Validates source batch and target write
#   - Avoids unpartitioned Window operations
#
# Existing Silver metadata columns:
#   _run_id
#   execution_date
#   from_date
#   to_date
#   ingested_at
#   store_id
#   store_name
#
# Bronze metadata columns:
#   _execution_date
#   _ingestion_mode
#   _from_date
#   _to_date
#   _week_start_date
#   _week_end_date
#   _run_id
#
# =============================================================================


from datetime import datetime, date
from delta.tables import DeltaTable

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    TimestampType,
    StringType
)


# =============================================================================
# 1. CONFIGURATION
# =============================================================================

BRONZE_TABLE = (
    f"{catalog_name}.{schema_name}.rtv_bronze"
)

SILVER_TABLE = (
    f"{catalog_name}.{schema_name}.rtv_silver"
)


print("=" * 100)
print("RTV BRONZE -> SILVER")
print("=" * 100)
print(f"Bronze table: {BRONZE_TABLE}")
print(f"Silver table: {SILVER_TABLE}")
print("=" * 100)


# =============================================================================
# 2. HELPER FUNCTIONS
# =============================================================================

def quote_identifier(column_name: str) -> str:
    """
    Safely quote a Spark SQL identifier.
    """
    return f"`{column_name.replace('`', '``')}`"


def sql_string_literal(value) -> str:
    """
    Convert a Python value into a SQL string literal.
    """
    if value is None:
        return "NULL"

    escaped_value = str(value).replace("'", "''")
    return f"'{escaped_value}'"


def sql_date_literal(value) -> str:
    """
    Convert a Python date/datetime/string into a SQL DATE literal.
    """
    if value is None:
        return "NULL"

    if isinstance(value, datetime):
        value = value.date()

    if isinstance(value, date):
        value = value.isoformat()

    value = str(value)[:10]

    return f"DATE '{value}'"


def normalize_date_column(column_name: str):
    """
    Convert a column into a date expression.

    Handles:
      - date
      - timestamp
      - string in YYYY-MM-DD format
    """
    return F.to_date(F.col(column_name))


def assert_table_exists(table_name: str):
    """
    Stop early if a required table does not exist.
    """
    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(
            f"Required table does not exist: {table_name}"
        )


def get_table_columns(table_name: str):
    """
    Return the table's column names in their existing order.
    """
    return [
        field.name
        for field in spark.table(table_name).schema.fields
    ]


# =============================================================================
# 3. VALIDATE TABLES
# =============================================================================

assert_table_exists(BRONZE_TABLE)
assert_table_exists(SILVER_TABLE)

bronze_df = spark.table(BRONZE_TABLE)
silver_df = spark.table(SILVER_TABLE)

bronze_columns = get_table_columns(BRONZE_TABLE)
silver_columns = get_table_columns(SILVER_TABLE)

silver_schema = silver_df.schema

print("Bronze columns:")
print(bronze_columns)

print("\nExisting Silver columns:")
print(silver_columns)


# =============================================================================
# 4. REQUIRED BRONZE METADATA COLUMNS
# =============================================================================

REQUIRED_BRONZE_METADATA_COLUMNS = [
    "_execution_date",
    "_from_date",
    "_to_date",
    "_run_id"
]

missing_bronze_metadata_columns = [
    column_name
    for column_name in REQUIRED_BRONZE_METADATA_COLUMNS
    if column_name not in bronze_columns
]

if missing_bronze_metadata_columns:
    raise RuntimeError(
        "Bronze table is missing required metadata columns: "
        f"{missing_bronze_metadata_columns}. "
        f"Available Bronze columns: {bronze_columns}"
    )


# =============================================================================
# 5. REQUIRED SILVER METADATA COLUMNS
#
# IMPORTANT:
# The existing Silver table does not contain:
#   ingestion_mode
#   week_start_date
#   week_end_date
#
# Therefore, those columns are NOT required here.
# =============================================================================

REQUIRED_SILVER_METADATA_COLUMNS = [
    "_run_id",
    "execution_date",
    "from_date",
    "to_date"
]

missing_silver_metadata_columns = [
    column_name
    for column_name in REQUIRED_SILVER_METADATA_COLUMNS
    if column_name not in silver_columns
]

if missing_silver_metadata_columns:
    raise RuntimeError(
        "Silver table is missing required metadata columns: "
        f"{missing_silver_metadata_columns}. "
        f"Available Silver columns: {silver_columns}"
    )


# =============================================================================
# 6. IDENTIFY AVAILABLE BRONZE BATCHES
#
# No Window operation is used here.
# =============================================================================

bronze_batch_inventory = (
    bronze_df
    .select(
        F.to_date(F.col("_execution_date")).alias(
            "_execution_date"
        ),
        F.to_date(F.col("_from_date")).alias(
            "_from_date"
        ),
        F.to_date(F.col("_to_date")).alias(
            "_to_date"
        ),
        F.col("_run_id").cast("string").alias(
            "_run_id"
        ),
        (
            F.col("_ingestion_mode")
            if "_ingestion_mode" in bronze_columns
            else F.lit(None).cast("string")
        ).alias("_ingestion_mode")
    )
    .where(
        F.col("_execution_date").isNotNull()
        & F.col("_from_date").isNotNull()
        & F.col("_to_date").isNotNull()
        & F.col("_run_id").isNotNull()
    )
    .groupBy(
        "_execution_date",
        "_from_date",
        "_to_date",
        "_run_id",
        "_ingestion_mode"
    )
    .agg(
        F.count(F.lit(1)).alias("row_count")
    )
    .orderBy(
        F.col("_execution_date").desc(),
        F.col("_from_date").desc(),
        F.col("_to_date").desc()
    )
)

batch_count = bronze_batch_inventory.count()

if batch_count == 0:
    raise RuntimeError(
        "No valid Bronze batches were found."
    )

print(f"Available Bronze batches: {batch_count}")

display(bronze_batch_inventory)


# =============================================================================
# 7. SELECT THE BATCH TO PROCESS
#
# The latest batch is selected.
#
# If you want to process a specific batch, set:
#
#   PROCESS_RUN_ID = "your-run-id"
#
#   PROCESS_EXECUTION_DATE = "YYYY-MM-DD"
#
# Otherwise, both values remain None and the latest batch is selected.
# =============================================================================

PROCESS_RUN_ID = None
PROCESS_EXECUTION_DATE = None


batch_selection_df = bronze_batch_inventory

if PROCESS_RUN_ID is not None:
    batch_selection_df = batch_selection_df.where(
        F.col("_run_id") == F.lit(PROCESS_RUN_ID)
    )

if PROCESS_EXECUTION_DATE is not None:
    batch_selection_df = batch_selection_df.where(
        F.col("_execution_date")
        == F.to_date(F.lit(PROCESS_EXECUTION_DATE))
    )

selected_batch_rows = (
    batch_selection_df
    .orderBy(
        F.col("_execution_date").desc(),
        F.col("_from_date").desc(),
        F.col("_to_date").desc()
    )
    .limit(1)
    .collect()
)

if not selected_batch_rows:
    raise RuntimeError(
        "No Bronze batch matched the requested selection criteria."
    )

selected_batch = selected_batch_rows[0]

selected_execution_date = selected_batch["_execution_date"]
selected_from_date = selected_batch["_from_date"]
selected_to_date = selected_batch["_to_date"]
selected_run_id = selected_batch["_run_id"]
selected_ingestion_mode = selected_batch["_ingestion_mode"]
selected_source_row_count = selected_batch["row_count"]


print("=" * 100)
print("SELECTED BRONZE BATCH")
print("=" * 100)
print(f"Execution date : {selected_execution_date}")
print(f"From date      : {selected_from_date}")
print(f"To date        : {selected_to_date}")
print(f"Run ID         : {selected_run_id}")
print(f"Ingestion mode : {selected_ingestion_mode}")
print(f"Source rows    : {selected_source_row_count}")
print("=" * 100)


# =============================================================================
# 8. FILTER THE SELECTED BRONZE BATCH
# =============================================================================

selected_bronze_df = (
    bronze_df
    .where(
        (
            F.to_date(F.col("_execution_date"))
            == F.lit(selected_execution_date)
        )
        &
        (
            F.to_date(F.col("_from_date"))
            == F.lit(selected_from_date)
        )
        &
        (
            F.to_date(F.col("_to_date"))
            == F.lit(selected_to_date)
        )
        &
        (
            F.col("_run_id").cast("string")
            == F.lit(str(selected_run_id))
        )
    )
)

actual_selected_row_count = selected_bronze_df.count()

if actual_selected_row_count == 0:
    raise RuntimeError(
        "The selected Bronze batch contains zero rows."
    )

if actual_selected_row_count != selected_source_row_count:
    raise RuntimeError(
        "Bronze batch row-count mismatch. "
        f"Inventory count: {selected_source_row_count}; "
        f"Filtered count: {actual_selected_row_count}"
    )

print(
    f"Validated selected Bronze rows: "
    f"{actual_selected_row_count}"
)


# =============================================================================
# 9. RENAME BRONZE METADATA COLUMNS TO SILVER NAMES
#
# Only metadata columns that already exist in Silver are generated.
# =============================================================================

silver_incoming = selected_bronze_df

# -------------------------------------------------------------------------
# _run_id -> _run_id
# -------------------------------------------------------------------------

silver_incoming = silver_incoming.withColumn(
    "_run_id",
    F.col("_run_id").cast("string")
)

# -------------------------------------------------------------------------
# _execution_date -> execution_date
# -------------------------------------------------------------------------

silver_incoming = silver_incoming.withColumn(
    "execution_date",
    F.to_date(F.col("_execution_date"))
)

# -------------------------------------------------------------------------
# _from_date -> from_date
# -------------------------------------------------------------------------

silver_incoming = silver_incoming.withColumn(
    "from_date",
    F.to_date(F.col("_from_date"))
)

# -------------------------------------------------------------------------
# _to_date -> to_date
# -------------------------------------------------------------------------

silver_incoming = silver_incoming.withColumn(
    "to_date",
    F.to_date(F.col("_to_date"))
)


# =============================================================================
# 10. MAP INGESTION TIMESTAMP
#
# Silver has ingested_at, while Bronze commonly has _ingested_at.
#
# If Bronze does not contain _ingested_at, the current timestamp is used.
# =============================================================================

if "ingested_at" in silver_columns:

    if "_ingested_at" in bronze_columns:

        silver_incoming = silver_incoming.withColumn(
            "ingested_at",
            F.col("_ingested_at")
        )

    elif "ingested_at" in bronze_columns:

        silver_incoming = silver_incoming.withColumn(
            "ingested_at",
            F.col("ingested_at")
        )

    else:

        silver_incoming = silver_incoming.withColumn(
            "ingested_at",
            F.current_timestamp()
        )


# =============================================================================
# 11. MAP STORE METADATA
#
# Silver has store_id and store_name.
# Bronze may contain either the regular or underscore-prefixed names.
# =============================================================================

if "store_id" in silver_columns:

    if "store_id" not in silver_incoming.columns:

        if "_store_id" in bronze_columns:

            silver_incoming = silver_incoming.withColumn(
                "store_id",
                F.col("_store_id")
            )

        else:

            silver_incoming = silver_incoming.withColumn(
                "store_id",
                F.lit(None).cast("string")
            )


if "store_name" in silver_columns:

    if "store_name" not in silver_incoming.columns:

        if "_store_name" in bronze_columns:

            silver_incoming = silver_incoming.withColumn(
                "store_name",
                F.col("_store_name")
            )

        elif "storeName" in bronze_columns:

            silver_incoming = silver_incoming.withColumn(
                "store_name",
                F.col("storeName")
            )

        else:

            silver_incoming = silver_incoming.withColumn(
                "store_name",
                F.lit(None).cast("string")
            )


# =============================================================================
# 12. REMOVE BRONZE-ONLY METADATA COLUMNS
#
# These columns are not present in the existing Silver schema.
# =============================================================================

bronze_only_metadata_columns = [
    "_execution_date",
    "_ingestion_mode",
    "_from_date",
    "_to_date",
    "_week_start_date",
    "_week_end_date"
]

for column_name in bronze_only_metadata_columns:

    if column_name in silver_incoming.columns:

        silver_incoming = silver_incoming.drop(column_name)


# =============================================================================
# 13. CHECK SILVER COLUMN COMPATIBILITY
# =============================================================================

incoming_columns = set(silver_incoming.columns)

missing_incoming_columns = [
    column_name
    for column_name in silver_columns
    if column_name not in incoming_columns
]

if missing_incoming_columns:

    raise RuntimeError(
        "Incoming Bronze batch is missing columns required by Silver: "
        f"{missing_incoming_columns}"
    )


# =============================================================================
# 14. ALIGN INCOMING DATA TO EXISTING SILVER SCHEMA
#
# This is the important fix for:
#
#   DELTA_FAILED_TO_MERGE_FIELDS
#
# Existing Silver types are used as the source of truth.
# =============================================================================

silver_schema_by_name = {
    field.name: field.dataType
    for field in silver_schema
}

aligned_columns = []

for field in silver_schema:

    column_name = field.name
    target_data_type = field.dataType

    aligned_expression = (
        F.col(column_name)
        .cast(target_data_type)
        .alias(column_name)
    )

    aligned_columns.append(aligned_expression)


silver_aligned_df = silver_incoming.select(
    *aligned_columns
)


# =============================================================================
# 15. VALIDATE REQUIRED METADATA AFTER CASTING
# =============================================================================

required_metadata_null_checks = (
    F.col("execution_date").isNull()
    | F.col("from_date").isNull()
    | F.col("to_date").isNull()
    | F.col("_run_id").isNull()
)

invalid_metadata_count = (
    silver_aligned_df
    .where(required_metadata_null_checks)
    .count()
)

if invalid_metadata_count > 0:

    raise RuntimeError(
        "Incoming Silver data contains rows with null required metadata. "
        f"Invalid rows: {invalid_metadata_count}"
    )


# =============================================================================
# 16. DATA QUALITY CHECKS
# =============================================================================

silver_aligned_row_count = silver_aligned_df.count()

if silver_aligned_row_count != actual_selected_row_count:

    raise RuntimeError(
        "Row count changed during Silver schema alignment. "
        f"Before alignment: {actual_selected_row_count}; "
        f"After alignment: {silver_aligned_row_count}"
    )


# Check for duplicate complete rows.
# This does not deduplicate the data because rtvId may be a header ID
# shared by multiple product lines.
duplicate_row_count = (
    silver_aligned_df
    .groupBy(*silver_columns)
    .count()
    .where(F.col("count") > 1)
    .count()
)

print(f"Duplicate complete-row groups: {duplicate_row_count}")

if duplicate_row_count > 0:

    print(
        "WARNING: Duplicate complete-row groups detected. "
        "No automatic deduplication was applied."
    )


# =============================================================================
# 17. PREVIEW ALIGNED DATA
# =============================================================================

print("=" * 100)
print("ALIGNED SILVER DATA")
print("=" * 100)

print(f"Row count: {silver_aligned_row_count}")
print("Schema:")
silver_aligned_df.printSchema()

display(silver_aligned_df.limit(20))


# =============================================================================
# 18. BUILD REPLACEMENT PREDICATE
#
# The predicate uses only columns that exist in Silver.
#
# IMPORTANT:
# _run_id is included because a Bronze batch is identified by its run ID.
# =============================================================================

replacement_predicate = (
    f"{quote_identifier('execution_date')} = "
    f"{sql_date_literal(selected_execution_date)} "
    f"AND {quote_identifier('from_date')} = "
    f"{sql_date_literal(selected_from_date)} "
    f"AND {quote_identifier('to_date')} = "
    f"{sql_date_literal(selected_to_date)} "
    f"AND {quote_identifier('_run_id')} = "
    f"{sql_string_literal(selected_run_id)}"
)

print("=" * 100)
print("REPLACEMENT PREDICATE")
print("=" * 100)
print(replacement_predicate)
print("=" * 100)


# =============================================================================
# 19. WRITE TO SILVER
#
# replaceWhere ensures that only the selected batch is replaced.
# Existing unrelated Silver batches are preserved.
# =============================================================================

(
    silver_aligned_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("replaceWhere", replacement_predicate)
    .option("mergeSchema", "false")
    .saveAsTable(SILVER_TABLE)
)

print("Silver batch write completed successfully.")


# =============================================================================
# 20. POST-WRITE VALIDATION
# =============================================================================

silver_after_write_df = spark.table(SILVER_TABLE)

validated_batch_df = (
    silver_after_write_df
    .where(
        (
            F.col("execution_date")
            == F.lit(selected_execution_date)
        )
        &
        (
            F.col("from_date")
            == F.lit(selected_from_date)
        )
        &
        (
            F.col("to_date")
            == F.lit(selected_to_date)
        )
        &
        (
            F.col("_run_id").cast("string")
            == F.lit(str(selected_run_id))
        )
    )
)

validated_row_count = validated_batch_df.count()

print("=" * 100)
print("POST-WRITE VALIDATION")
print("=" * 100)
print(f"Expected rows: {silver_aligned_row_count}")
print(f"Actual rows  : {validated_row_count}")
print("=" * 100)

if validated_row_count != silver_aligned_row_count:

    raise RuntimeError(
        "Post-write validation failed. "
        f"Expected {silver_aligned_row_count} rows, "
        f"but found {validated_row_count} rows."
    )


# =============================================================================
# 21. VALIDATE THAT THE SELECTED BATCH HAS NO NULL METADATA
# =============================================================================

post_write_invalid_metadata_count = (
    validated_batch_df
    .where(
        F.col("execution_date").isNull()
        | F.col("from_date").isNull()
        | F.col("to_date").isNull()
        | F.col("_run_id").isNull()
    )
    .count()
)

if post_write_invalid_metadata_count > 0:

    raise RuntimeError(
        "Post-write validation found rows with null metadata. "
        f"Invalid rows: {post_write_invalid_metadata_count}"
    )


# =============================================================================
# 22. FINAL SUMMARY
# =============================================================================

print("=" * 100)
print("RTV SILVER PIPELINE COMPLETED SUCCESSFULLY")
print("=" * 100)
print(f"Silver table       : {SILVER_TABLE}")
print(f"Execution date     : {selected_execution_date}")
print(f"From date          : {selected_from_date}")
print(f"To date            : {selected_to_date}")
print(f"Run ID             : {selected_run_id}")
print(f"Ingestion mode     : {selected_ingestion_mode}")
print(f"Rows written       : {silver_aligned_row_count}")
print(f"Rows validated     : {validated_row_count}")
print(f"Duplicate groups   : {duplicate_row_count}")
print("=" * 100)

# COMMAND ----------

