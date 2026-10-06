# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# ============================================================
# 22_fact_stock
# CANONICAL STOCK FACT TABLE
#
# Grain:
#   1 row per product × store × snapshot_date
#
# STORE RULE:
#   Only stores present in dim_store are processed.
#
# Store matching:
#   normalize(stock.store_name)
#       ->
#   dim_store.normalized_store_name
#
# NO HARDCODED STORE IDs / STORE NAMES
#
# Source:
#   billing.sales_data.stock_silver
#
# Dimensions:
#   billing.sales_data.dim_product
#   billing.sales_data.dim_store
#   billing.sales_data.dim_date
#
# Target:
#   billing.sales_data.fact_stock
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# 1. CONFIG
# ============================================================

CATALOG = "billing"
SCHEMA = "sales_data"

STOCK_SOURCE = f"{CATALOG}.{SCHEMA}.stock_silver"

PRODUCT_DIM = f"{CATALOG}.{SCHEMA}.dim_product"
STORE_DIM = f"{CATALOG}.{SCHEMA}.dim_store"
DATE_DIM = f"{CATALOG}.{SCHEMA}.dim_date"

TARGET_TABLE = f"{CATALOG}.{SCHEMA}.fact_stock"


print("=" * 80)
print("22 FACT STOCK")
print("=" * 80)

print(f"Stock source : {STOCK_SOURCE}")
print(f"Product dim  : {PRODUCT_DIM}")
print(f"Store dim    : {STORE_DIM}")
print(f"Date dim     : {DATE_DIM}")
print(f"Target       : {TARGET_TABLE}")


# ============================================================
# 2. STORE NAME NORMALIZATION
# ============================================================
#
# Same normalization used by 16_dim_store.
#
# Handles:
#   - leading spaces
#   - trailing spaces
#   - repeated spaces
#   - tabs
#   - newlines
#   - carriage returns
#   - non-breaking spaces
#   - uppercase/lowercase
#
# Example:
#
#   " Vijay   Nagar "
#   "VIJAY NAGAR"
#   "Vijay Nagar\t"
#
# -> "vijay nagar"
# ============================================================

def normalize_store_name(column):

    return F.lower(
        F.trim(
            F.regexp_replace(
                F.regexp_replace(
                    column.cast("string"),
                    r"[\t\r\n\u00A0]+",
                    " "
                ),
                r"\s+",
                " "
            )
        )
    )


# ============================================================
# 3. LOAD STOCK
# ============================================================

stock_raw = spark.table(
    STOCK_SOURCE
)

raw_stock_count = stock_raw.count()

print()
print(
    f"Raw stock rows: {raw_stock_count:,}"
)


# ============================================================
# 4. NORMALIZE STOCK
# ============================================================

stock = (
    stock_raw

    # --------------------------------------------------------
    # Original source store information
    # --------------------------------------------------------

    .withColumn(
        "_stock_store_id",
        F.col("_store_id").cast("string")
    )

    .withColumn(
        "_stock_store_name",
        F.col("store_name").cast("string")
    )

    # --------------------------------------------------------
    # Canonical store matching field
    # --------------------------------------------------------

    .withColumn(
        "_normalized_store_name",
        normalize_store_name(
            F.col("store_name")
        )
    )

    # --------------------------------------------------------
    # Barcode
    # --------------------------------------------------------
    #
    # Keep barcode as STRING.
    #
    # Do not cast to bigint because leading zeros must not
    # disappear.
    #
    # Remove only a trailing ".0" when present.
    # --------------------------------------------------------

    .withColumn(
        "_barcode",
        F.when(
            F.col("barcode").isNotNull(),
            F.trim(
                F.regexp_replace(
                    F.col("barcode").cast("string"),
                    r"\.0$",
                    ""
                )
            )
        )
    )

    # --------------------------------------------------------
    # Snapshot date
    # --------------------------------------------------------

    .withColumn(
        "_snapshot_date_clean",
        F.to_date(
            F.col("_snapshot_date").cast("string")
        )
    )

    # --------------------------------------------------------
    # Measures
    # --------------------------------------------------------

    .withColumn(
        "_quantity",
        F.coalesce(
            F.col("quantity").cast("double"),
            F.lit(0.0)
        )
    )

    .withColumn(
        "_selling_price",
        F.col("selling_price").cast("double")
    )

    .withColumn(
        "_printed_mrp",
        F.col("printed_mrp").cast("double")
    )

    .withColumn(
        "_cost_price",
        F.col("cost_price").cast("double")
    )

    .withColumn(
        "_total_amount",
        F.col("total_amount").cast("double")
    )
)


# ============================================================
# 5. SOURCE VALIDATION
# ============================================================
#
# IMPORTANT:
#
# _stock_store_id is NOT part of the canonical identity.
#
# A stock row is valid if:
#
#   barcode exists
#   normalized store name exists
#   snapshot date exists
#
# Store matching is performed against dim_store.
# ============================================================

invalid_source_rows = (
    stock
    .filter(

        (
            F.col("_barcode").isNull()
            |
            (F.col("_barcode") == "")
        )

        |

        (
            F.col("_normalized_store_name").isNull()
            |
            (F.col("_normalized_store_name") == "")
        )

        |

        F.col("_snapshot_date_clean").isNull()
    )
    .count()
)


print()
print(
    f"Invalid stock identity rows: "
    f"{invalid_source_rows:,}"
)


if invalid_source_rows > 0:

    display(
        stock
        .filter(
            (
                F.col("_barcode").isNull()
                |
                (F.col("_barcode") == "")
            )
            |
            (
                F.col("_normalized_store_name").isNull()
                |
                (F.col("_normalized_store_name") == "")
            )
            |
            F.col("_snapshot_date_clean").isNull()
        )
        .limit(50)
    )

    raise RuntimeError(
        f"❌ {invalid_source_rows:,} stock rows contain "
        "invalid barcode/store/date identity values."
    )


# ============================================================
# 6. LOAD DIM_STORE
# ============================================================

dim_store_raw = spark.table(
    STORE_DIM
)


if "normalized_store_name" not in dim_store_raw.columns:

    raise RuntimeError(
        "❌ dim_store does not contain "
        "'normalized_store_name'. "
        "Run 16_dim_store first."
    )


dim_store = (
    dim_store_raw

    .select(
        F.col("store_key").alias("lookup_store_key"),
        F.col("store_id").alias("lookup_store_id"),
        F.col("store_name").alias("lookup_store_name"),
        F.col("normalized_store_name").alias(
            "lookup_normalized_store_name"
        )
    )
)


# ============================================================
# 7. DIM_STORE VALIDATION
# ============================================================

duplicate_store_keys = (
    dim_store
    .groupBy(
        "lookup_store_key"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


if duplicate_store_keys > 0:

    raise RuntimeError(
        f"❌ dim_store contains "
        f"{duplicate_store_keys:,} duplicate store keys."
    )


duplicate_normalized_names = (
    dim_store
    .groupBy(
        "lookup_normalized_store_name"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


if duplicate_normalized_names > 0:

    raise RuntimeError(
        f"❌ dim_store contains "
        f"{duplicate_normalized_names:,} duplicate "
        "normalized store names."
    )


dim_store_count = (
    dim_store.count()
)


print()
print(
    f"Stores in dim_store: "
    f"{dim_store_count:,}"
)


# ============================================================
# 8. FILTER STOCK TO DIM_STORE STORES
# ============================================================
#
# dim_store is the ONLY operational store authority.
#
# No billing dependency.
# No hard-coded exclusions.
# No hard-coded store IDs.
# No hard-coded store names.
# ============================================================

stock_operational = (
    stock.alias("s")
    .join(
        dim_store
        .select(
            "lookup_normalized_store_name"
        )
        .distinct()
        .alias("d"),

        F.col("s._normalized_store_name")
        ==
        F.col("d.lookup_normalized_store_name"),

        "left_semi"
    )
)


operational_stock_count = (
    stock_operational.count()
)


excluded_stock_count = (
    raw_stock_count
    -
    operational_stock_count
)


print()
print("=" * 80)
print("STORE FILTER")
print("=" * 80)

print(
    f"Raw stock rows       : "
    f"{raw_stock_count:,}"
)

print(
    f"Accepted stock rows  : "
    f"{operational_stock_count:,}"
)

print(
    f"Excluded stock rows  : "
    f"{excluded_stock_count:,}"
)


if (
    operational_stock_count
    +
    excluded_stock_count
    !=
    raw_stock_count
):

    raise RuntimeError(
        "❌ Stock store filtering does not reconcile."
    )


# ============================================================
# 9. DIAGNOSTIC — EXCLUDED STORES
# ============================================================
#
# Informational only.
#
# These stores are NOT processed because they are absent from
# dim_store.
#
# No hard-coded exclusions.
# ============================================================

excluded_stock = (
    stock.alias("s")
    .join(
        dim_store
        .select(
            "lookup_normalized_store_name"
        )
        .distinct()
        .alias("d"),

        F.col("s._normalized_store_name")
        ==
        F.col("d.lookup_normalized_store_name"),

        "left_anti"
    )
)


print()
print(
    "Stores excluded because they are absent from dim_store:"
)

display(
    excluded_stock
    .groupBy(
        "_stock_store_id",
        "_stock_store_name",
        "_normalized_store_name"
    )
    .agg(
        F.count("*").alias(
            "stock_rows"
        )
    )
    .orderBy(
        F.desc("stock_rows")
    )
)


# ============================================================
# 10. LOAD PRODUCT DIMENSION
# ============================================================
#
# dim_product uses BARCODE as the canonical product identity.
#
# IMPORTANT:
#   Use explicit lookup column names.
#
# We do NOT keep a second generic "_barcode" column inside
# the joined DataFrame.
#
# This prevents ambiguous-column errors after the join.
# ============================================================

dim_product = (
    spark.table(
        PRODUCT_DIM
    )

    .select(
        F.col("product_key").alias(
            "lookup_product_key"
        ),

        F.col("barcode")
            .cast("string")
            .alias("raw_lookup_barcode")
    )

    .withColumn(
        "lookup_barcode",
        F.when(
            F.col("raw_lookup_barcode").isNotNull(),

            F.trim(
                F.regexp_replace(
                    F.col("raw_lookup_barcode"),
                    r"\.0$",
                    ""
                )
            )
        )
    )

    .filter(
        F.col("lookup_barcode").isNotNull()
        &
        (F.col("lookup_barcode") != "")
    )

    .select(
        "lookup_product_key",
        "lookup_barcode"
    )
)


# ============================================================
# 11. PRODUCT DIMENSION VALIDATION
# ============================================================

duplicate_product_barcodes = (
    dim_product
    .groupBy(
        "lookup_barcode"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


if duplicate_product_barcodes > 0:

    display(
        dim_product
        .groupBy(
            "lookup_barcode"
        )
        .count()
        .filter(
            F.col("count") > 1
        )
        .orderBy(
            F.desc("count")
        )
        .limit(50)
    )

    raise RuntimeError(
        f"❌ dim_product contains "
        f"{duplicate_product_barcodes:,} duplicate "
        "barcode mappings."
    )


product_lookup_count = (
    dim_product.count()
)


print()
print(
    f"Product barcode lookup rows: "
    f"{product_lookup_count:,}"
)


# ============================================================
# 12. PRODUCT LOOKUP
# ============================================================
#
# Join first.
# Immediately project the required columns.
#
# This is the critical fix for the original code.
# ============================================================

stock_product = (
    stock_operational.alias("s")

    .join(
        dim_product.alias("p"),

        F.col("s._barcode")
        ==
        F.col("p.lookup_barcode"),

        "left"
    )

    .select(

        # Keep the complete stock row
        F.col("s.*"),

        # Keep only the canonical product key
        F.col("p.lookup_product_key")
            .alias("product_key")
    )
)


# ============================================================
# 13. PRODUCT MATCH VALIDATION
# ============================================================

unmatched_products = (
    stock_product
    .filter(
        F.col("product_key").isNull()
    )
    .count()
)


print()
print(
    f"Unmatched product barcodes: "
    f"{unmatched_products:,}"
)


if unmatched_products > 0:

    print(
        "Sample unmatched products:"
    )

    display(
        stock_product
        .filter(
            F.col("product_key").isNull()
        )
        .select(
            "_barcode",
            "product_id",
            "product_name"
        )
        .distinct()
        .limit(50)
    )

    raise RuntimeError(
        f"❌ {unmatched_products:,} stock rows could not "
        "be mapped to dim_product."
    )


# ============================================================
# 14. STORE LOOKUP
# ============================================================
#
# The stock has already been filtered using dim_store.
#
# Now perform the actual lookup and immediately project the
# required columns.
# ============================================================

stock_store = (
    stock_product.alias("s")

    .join(
        dim_store.alias("st"),

        F.col("s._normalized_store_name")
        ==
        F.col("st.lookup_normalized_store_name"),

        "left"
    )

    .select(

        F.col("s.*"),

        F.col("st.lookup_store_key")
            .alias("store_key"),

        F.col("st.lookup_store_id")
            .alias("dim_store_id"),

        F.col("st.lookup_store_name")
            .alias("dim_store_name")
    )
)


# ============================================================
# 15. STORE LOOKUP VALIDATION
# ============================================================

unmatched_stores = (
    stock_store
    .filter(
        F.col("store_key").isNull()
    )
    .count()
)


print(
    f"Unmatched stores after dim_store filtering: "
    f"{unmatched_stores:,}"
)


if unmatched_stores > 0:

    display(
        stock_store
        .filter(
            F.col("store_key").isNull()
        )
        .groupBy(
            "_stock_store_id",
            "_stock_store_name",
            "_normalized_store_name"
        )
        .count()
        .orderBy(
            F.desc("count")
        )
    )

    raise RuntimeError(
        f"❌ {unmatched_stores:,} rows still failed "
        "the dim_store lookup."
    )


# ============================================================
# 16. LOAD DATE DIMENSION
# ============================================================

dim_date = (
    spark.table(
        DATE_DIM
    )

    .select(
        F.col("date_key").alias(
            "lookup_date_key"
        ),

        F.col("date").alias(
            "lookup_date"
        )
    )
)


# ============================================================
# 17. DATE DIMENSION VALIDATION
# ============================================================

duplicate_dates = (
    dim_date
    .groupBy(
        "lookup_date"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


if duplicate_dates > 0:

    raise RuntimeError(
        f"❌ dim_date contains "
        f"{duplicate_dates:,} duplicate dates."
    )


# ============================================================
# 18. DATE LOOKUP
# ============================================================
#
# Immediately project after the join.
# ============================================================

stock_date = (
    stock_store.alias("s")

    .join(
        dim_date.alias("d"),

        F.col("s._snapshot_date_clean")
        ==
        F.col("d.lookup_date"),

        "left"
    )

    .select(

        F.col("s.*"),

        F.col("d.lookup_date_key")
            .alias("date_key")
    )
)


# ============================================================
# 19. DATE LOOKUP VALIDATION
# ============================================================

unmatched_dates = (
    stock_date
    .filter(
        F.col("date_key").isNull()
    )
    .count()
)


print(
    f"Unmatched snapshot dates: "
    f"{unmatched_dates:,}"
)


if unmatched_dates > 0:

    display(
        stock_date
        .filter(
            F.col("date_key").isNull()
        )
        .select(
            "_snapshot_date_clean"
        )
        .distinct()
        .orderBy(
            "_snapshot_date_clean"
        )
    )

    raise RuntimeError(
        f"❌ {unmatched_dates:,} stock rows could "
        "not be mapped to dim_date."
    )


# ============================================================
# 20. SOURCE GRAIN ANALYSIS
# ============================================================
#
# Canonical fact grain:
#
#   1 row per product × store × snapshot_date
#
# IMPORTANT:
# We ONLY group by the canonical grain keys.
#
# Do NOT include:
#   barcode
#   product_id
#   product_name
#   vendor_name
#   category_name
#   store name
#   etc.
#
# Those are attributes and must be aggregated.
# ============================================================

source_grain_check = (
    stock_date

    .groupBy(
        "product_key",
        "store_key",
        "date_key",
        "_snapshot_date_clean"
    )

    .count()
)


duplicate_source_grain = (
    source_grain_check

    .filter(
        F.col("count") > 1
    )
)


duplicate_source_grain_count = (
    duplicate_source_grain.count()
)


duplicate_source_rows = (
    duplicate_source_grain

    .agg(
        F.sum(
            F.col("count") - F.lit(1)
        ).alias("duplicate_rows")
    )

    .collect()[0]["duplicate_rows"]
)


duplicate_source_rows = (
    int(duplicate_source_rows)
    if duplicate_source_rows is not None
    else 0
)


print()
print("=" * 80)
print("SOURCE GRAIN ANALYSIS")
print("=" * 80)

print(
    f"Accepted source rows         : "
    f"{operational_stock_count:,}"
)

print(
    f"Duplicate grain combinations : "
    f"{duplicate_source_grain_count:,}"
)

print(
    f"Additional duplicate rows    : "
    f"{duplicate_source_rows:,}"
)


if duplicate_source_grain_count > 0:

    print()
    print(
        "Duplicate source grain records:"
    )

    display(
        duplicate_source_grain

        .orderBy(
            F.desc("count")
        )

        .limit(50)
    )


# ============================================================
# 21. AGGREGATE TO EXACT CANONICAL FACT GRAIN
# ============================================================
#
# THIS IS THE IMPORTANT FIX.
#
# GROUP ONLY BY:
#
#   product_key
#   store_key
#   date_key
#   snapshot_date
#
# Every other field is an attribute/measure and is aggregated.
#
# Therefore the resulting DataFrame CANNOT contain duplicate
# product × store × snapshot_date combinations.
# ============================================================

fact_stock = (

    stock_date

    .groupBy(

        # ----------------------------------------------------
        # ONLY CANONICAL GRAIN KEYS
        # ----------------------------------------------------

        "product_key",
        "store_key",
        "date_key",
        "_snapshot_date_clean"
    )

    .agg(

        # ====================================================
        # PRODUCT ATTRIBUTES
        # ====================================================

        F.first(
            "_barcode",
            ignorenulls=True
        ).alias(
            "barcode"
        ),

        F.first(
            F.col("product_id").cast("string"),
            ignorenulls=True
        ).alias(
            "source_product_id"
        ),

        F.first(
            "product_name",
            ignorenulls=True
        ).alias(
            "product_name"
        ),


        # ====================================================
        # STORE LINEAGE
        # ====================================================

        F.first(
            "_stock_store_id",
            ignorenulls=True
        ).alias(
            "source_store_id"
        ),

        F.first(
            "_stock_store_name",
            ignorenulls=True
        ).alias(
            "source_store_name"
        ),

        F.first(
            "_normalized_store_name",
            ignorenulls=True
        ).alias(
            "source_normalized_store_name"
        ),

        F.first(
            F.col("dim_store_id").cast("string"),
            ignorenulls=True
        ).alias(
            "dim_store_id"
        ),

        F.first(
            "dim_store_name",
            ignorenulls=True
        ).alias(
            "dim_store_name"
        ),


        # ====================================================
        # OPERATIONAL ATTRIBUTES
        # ====================================================

        F.first(
            "vendor_name",
            ignorenulls=True
        ).alias(
            "vendor_name"
        ),

        F.first(
            "category_name",
            ignorenulls=True
        ).alias(
            "category_name"
        ),

        F.first(
            "sub_category_of",
            ignorenulls=True
        ).alias(
            "sub_category"
        ),


        # ====================================================
        # MEASURES
        # ====================================================

        # Duplicate stock quantities are additive.
        F.sum(
            "_quantity"
        ).alias(
            "quantity"
        ),

        # Duplicate total amounts are additive.
        F.sum(
            "_total_amount"
        ).alias(
            "total_amount"
        ),

        # Price fields are attributes, not additive measures.
        F.max(
            "_selling_price"
        ).alias(
            "selling_price"
        ),

        F.max(
            "_printed_mrp"
        ).alias(
            "printed_mrp"
        ),

        F.max(
            "_cost_price"
        ).alias(
            "cost_price"
        ),


        # ====================================================
        # LINEAGE
        # ====================================================

        F.max(
            F.col("_run_id").cast("string")
        ).alias(
            "source_run_id"
        ),

        F.max(
            "_ingested_at"
        ).alias(
            "ingested_at"
        ),

        F.max(
            "_processed_date"
        ).alias(
            "processed_date"
        )
    )

    .select(

        # ----------------------------------------------------
        # DIMENSION KEYS
        # ----------------------------------------------------

        F.col("product_key"),

        F.col("store_key"),

        F.col("date_key"),

        F.col("_snapshot_date_clean")
            .alias(
                "snapshot_date"
            ),


        # ----------------------------------------------------
        # PRODUCT
        # ----------------------------------------------------

        F.col("barcode"),

        F.col("source_product_id"),

        F.col("product_name"),


        # ----------------------------------------------------
        # STORE
        # ----------------------------------------------------

        F.col("source_store_id"),

        F.col("source_store_name"),

        F.col("source_normalized_store_name"),

        F.col("dim_store_id"),

        F.col("dim_store_name"),


        # ----------------------------------------------------
        # OPERATIONAL ATTRIBUTES
        # ----------------------------------------------------

        F.col("vendor_name"),

        F.col("category_name"),

        F.col("sub_category"),


        # ----------------------------------------------------
        # MEASURES
        # ----------------------------------------------------

        F.col("quantity")
            .cast("double")
            .alias("quantity"),

        F.col("selling_price")
            .cast("double")
            .alias("selling_price"),

        F.col("printed_mrp")
            .cast("double")
            .alias("printed_mrp"),

        F.col("cost_price")
            .cast("double")
            .alias("cost_price"),

        F.col("total_amount")
            .cast("double")
            .alias("total_amount"),


        # ----------------------------------------------------
        # LINEAGE
        # ----------------------------------------------------

        F.col("source_run_id"),

        F.col("ingested_at"),

        F.col("processed_date")
    )
)


# ============================================================
# 22. FACT ROW COUNT
# ============================================================

fact_count = (
    fact_stock.count()
)


print()
print("=" * 80)
print("FACT AGGREGATION")
print("=" * 80)

print(
    f"Accepted source rows : "
    f"{operational_stock_count:,}"
)

print(
    f"Aggregated fact rows : "
    f"{fact_count:,}"
)

print(
    f"Rows consolidated    : "
    f"{operational_stock_count - fact_count:,}"
)


# ============================================================
# 23. NULL DIMENSION KEY VALIDATION
# ============================================================

null_key_rows = (
    fact_stock

    .filter(
        F.col("product_key").isNull()
        |
        F.col("store_key").isNull()
        |
        F.col("date_key").isNull()
    )

    .count()
)


if null_key_rows > 0:

    display(
        fact_stock

        .filter(
            F.col("product_key").isNull()
            |
            F.col("store_key").isNull()
            |
            F.col("date_key").isNull()
        )

        .limit(50)
    )

    raise RuntimeError(
        f"❌ {null_key_rows:,} fact_stock rows contain "
        "NULL dimension keys."
    )


# ============================================================
# 24. FINAL FACT GRAIN VALIDATION
# ============================================================
#
# Because Section 21 groups ONLY by the canonical grain,
# duplicates should now be impossible.
# ============================================================

duplicate_grain = (
    fact_stock

    .groupBy(
        "product_key",
        "store_key",
        "snapshot_date"
    )

    .count()

    .filter(
        F.col("count") > 1
    )
)


duplicate_grain_count = (
    duplicate_grain.count()
)


print()
print(
    f"Final duplicate product × store × date combinations: "
    f"{duplicate_grain_count:,}"
)


if duplicate_grain_count > 0:

    display(
        duplicate_grain

        .orderBy(
            F.desc("count")
        )

        .limit(20)
    )

    raise RuntimeError(
        f"❌ Found {duplicate_grain_count:,} duplicate "
        "fact_stock grain combinations after canonical "
        "aggregation."
    )


# ============================================================
# 25. SOURCE → FACT RECONCILIATION
# ============================================================

if fact_count > operational_stock_count:

    raise RuntimeError(
        "❌ Fact row count cannot exceed accepted source rows."
    )


expected_consolidated_rows = (
    operational_stock_count
    -
    fact_count
)


print()
print("=" * 80)
print("SOURCE → FACT RECONCILIATION")
print("=" * 80)

print(
    f"Raw source rows       : "
    f"{raw_stock_count:,}"
)

print(
    f"Excluded rows         : "
    f"{excluded_stock_count:,}"
)

print(
    f"Accepted source rows  : "
    f"{operational_stock_count:,}"
)

print(
    f"Final fact rows       : "
    f"{fact_count:,}"
)

print(
    f"Rows consolidated     : "
    f"{expected_consolidated_rows:,}"
)


# ============================================================
# 26. REBUILD TARGET
# ============================================================

if spark.catalog.tableExists(
    TARGET_TABLE
):

    print()
    print(
        f"⚠️ Existing {TARGET_TABLE} detected."
    )

    spark.sql(
        f"DROP TABLE {TARGET_TABLE}"
    )

    print(
        "Existing fact_stock dropped."
    )


# ============================================================
# 27. WRITE DELTA
# ============================================================

(
    fact_stock

    .write
    .format("delta")
    .mode("overwrite")

    .saveAsTable(
        TARGET_TABLE
    )
)


print()
print(
    "✅ fact_stock written successfully."
)


# ============================================================
# 28. POST-WRITE VALIDATION
# ============================================================

final_fact = (
    spark.table(
        TARGET_TABLE
    )
)


final_count = (
    final_fact.count()
)


final_null_keys = (
    final_fact

    .filter(
        F.col("product_key").isNull()
        |
        F.col("store_key").isNull()
        |
        F.col("date_key").isNull()
    )

    .count()
)


final_duplicates = (
    final_fact

    .groupBy(
        "product_key",
        "store_key",
        "snapshot_date"
    )

    .count()

    .filter(
        F.col("count") > 1
    )

    .count()
)


# ============================================================
# 29. FINAL ASSERTIONS
# ============================================================

if final_count != fact_count:

    raise RuntimeError(
        f"❌ Final row count mismatch. "
        f"Expected {fact_count:,}, "
        f"got {final_count:,}."
    )


if final_null_keys > 0:

    raise RuntimeError(
        f"❌ Final fact contains "
        f"{final_null_keys:,} NULL dimension keys."
    )


if final_duplicates > 0:

    raise RuntimeError(
        f"❌ Final fact contains "
        f"{final_duplicates:,} duplicate grain keys."
    )


# ============================================================
# 30. FINAL METRICS
# ============================================================

snapshot_range = (
    final_fact

    .agg(
        F.min(
            "snapshot_date"
        ).alias(
            "min_date"
        ),

        F.max(
            "snapshot_date"
        ).alias(
            "max_date"
        )
    )

    .collect()[0]
)


store_count = (
    final_fact

    .select(
        "store_key"
    )

    .distinct()

    .count()
)


product_count = (
    final_fact

    .select(
        "product_key"
    )

    .distinct()

    .count()
)


# ============================================================
# 31. FINAL SUCCESS
# ============================================================

print()
print("=" * 80)
print("22 FACT STOCK SUCCESS")
print("=" * 80)

print(
    f"Raw stock rows          : "
    f"{raw_stock_count:,}"
)

print(
    f"Accepted source rows    : "
    f"{operational_stock_count:,}"
)

print(
    f"Excluded stock rows     : "
    f"{excluded_stock_count:,}"
)

print(
    f"Rows consolidated       : "
    f"{expected_consolidated_rows:,}"
)

print(
    f"Final fact rows         : "
    f"{final_count:,}"
)

print(
    f"Stores represented      : "
    f"{store_count:,}"
)

print(
    f"Products represented    : "
    f"{product_count:,}"
)

print(
    f"Snapshot range          : "
    f"{snapshot_range['min_date']} → "
    f"{snapshot_range['max_date']}"
)

print(
    "Grain                   : "
    "product × store × snapshot_date"
)

print(
    "Store authority         : "
    "dim_store"
)

print(
    "Store matching          : "
    "normalized_store_name"
)

print(
    "Product authority       : "
    "dim_product.barcode"
)

print(
    "Date authority          : "
    "dim_date"
)

print(
    f"Target                  : "
    f"{TARGET_TABLE}"
)

print("=" * 80)


# ============================================================
# 32. SAMPLE
# ============================================================

display(
    final_fact

    .orderBy(
        F.col("snapshot_date").desc()
    )

    .limit(20)
)

# COMMAND ----------

