# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    trim,
    lit,
    count,
    sum as spark_sum
)
from datetime import datetime

# ============================================================
# GOLD STOCK
# ============================================================

SOURCE_TABLE = f"{catalog_name}.{schema_name}.stock_silver"
TARGET_TABLE = f"{catalog_name}.{schema_name}.gold_stock"

print("=" * 70)
print("GOLD STOCK")
print("=" * 70)
print(f"Source table : {SOURCE_TABLE}")
print(f"Target table : {TARGET_TABLE}")
print(f"Target date  : {TARGET_DATE}")
print("=" * 70)


# ============================================================
# 1. CHECK SOURCE TABLE
# ============================================================

if not spark.catalog.tableExists(SOURCE_TABLE):
    raise RuntimeError(
        f"❌ Source table does not exist: {SOURCE_TABLE}"
    )

silver = spark.table(SOURCE_TABLE)

silver_count = silver.count()

print(f"📥 Silver stock rows: {silver_count:,}")

if silver_count == 0:
    raise RuntimeError(
        f"❌ No Silver stock data found for {TARGET_DATE}. "
        f"Did 10_ingest_stock and 11_silver_stock run successfully?"
    )


# ============================================================
# 2. REQUIRED COLUMNS
# ============================================================

required_columns = [
    "product_id",
    "product_name",
    "barcode",
    "quantity",
    "selling_price",
    "printed_mrp",
    "cost_price",
    "total_amount",
    "store_name",
    "vendor_name",
    "category_name",
    "sub_category_of",
    "brand",
    "_store_id",
    "_store_name",
    "_snapshot_date",
    "_run_id",
    "_ingested_at",
    "_processed_date"
]

missing_columns = [
    c for c in required_columns
    if c not in silver.columns
]

if missing_columns:
    raise RuntimeError(
        f"❌ Missing required stock columns: {missing_columns}"
    )


# ============================================================
# 3. FILTER TARGET SNAPSHOT
# ============================================================

stock_df = (
    silver
    .filter(col("_snapshot_date") == lit(TARGET_DATE))
)

snapshot_count = stock_df.count()

print(
    f"📅 Silver rows for snapshot {TARGET_DATE}: "
    f"{snapshot_count:,}"
)

if snapshot_count == 0:
    raise RuntimeError(
        f"❌ No Silver stock rows found for snapshot_date={TARGET_DATE}"
    )


# ============================================================
# 4. NORMALIZE STOCK DATA
# ============================================================

stock_df = (
    stock_df

    # --------------------------------------------------------
    # Product fields
    # --------------------------------------------------------

    .withColumn(
        "product_id",
        trim(col("product_id").cast("string"))
    )

    .withColumn(
        "product_name",
        trim(col("product_name").cast("string"))
    )

    .withColumn(
        "barcode",
        trim(col("barcode").cast("string"))
    )

    # --------------------------------------------------------
    # Store fields
    # --------------------------------------------------------

    .withColumn(
        "store_name",
        trim(col("store_name").cast("string"))
    )

    .withColumn(
        "_store_id",
        trim(col("_store_id").cast("string"))
    )

    .withColumn(
        "_store_name",
        trim(col("_store_name").cast("string"))
    )

    # --------------------------------------------------------
    # Other descriptive fields
    # --------------------------------------------------------

    .withColumn(
        "vendor_name",
        trim(col("vendor_name").cast("string"))
    )

    .withColumn(
        "category_name",
        trim(col("category_name").cast("string"))
    )

    .withColumn(
        "sub_category_of",
        trim(col("sub_category_of").cast("string"))
    )

    .withColumn(
        "brand",
        trim(col("brand").cast("string"))
    )

    # --------------------------------------------------------
    # Numeric fields
    # --------------------------------------------------------

    .withColumn(
        "quantity",
        col("quantity").cast("double")
    )

    .withColumn(
        "selling_price",
        col("selling_price").cast("double")
    )

    .withColumn(
        "printed_mrp",
        col("printed_mrp").cast("double")
    )

    .withColumn(
        "cost_price",
        col("cost_price").cast("double")
    )

    .withColumn(
        "total_amount",
        col("total_amount").cast("double")
    )

    # --------------------------------------------------------
    # Gold metadata
    # --------------------------------------------------------

    .withColumn(
        "source_date",
        col("_snapshot_date")
    )
)


# ============================================================
# 5. BASIC VALIDATION
# ============================================================

# ------------------------------------------------------------
# Snapshot date validation
# ------------------------------------------------------------

invalid_snapshot_count = (
    stock_df
    .filter(col("_snapshot_date").isNull())
    .count()
)

if invalid_snapshot_count > 0:
    raise RuntimeError(
        f"❌ {invalid_snapshot_count:,} rows have NULL "
        f"_snapshot_date."
    )


# ------------------------------------------------------------
# Store validation
# ------------------------------------------------------------

invalid_store_count = (
    stock_df
    .filter(
        col("store_name").isNull() |
        (col("store_name") == "")
    )
    .count()
)

if invalid_store_count > 0:
    raise RuntimeError(
        f"❌ {invalid_store_count:,} rows have "
        f"NULL/empty store_name."
    )


# ------------------------------------------------------------
# Product validation
# ------------------------------------------------------------

invalid_product_count = (
    stock_df
    .filter(
        col("product_id").isNull() |
        (col("product_id") == "")
    )
    .count()
)

if invalid_product_count > 0:
    print(
        f"⚠️ Warning: {invalid_product_count:,} rows have "
        f"NULL/empty product_id."
    )


# ============================================================
# 6. REMOVE EXACT DUPLICATES
# ============================================================
#
# Stock API responses can potentially contain duplicate rows.
#
# We remove only exact duplicate product-stock records.
#
# We DO NOT aggregate stock quantities here.
#
# ============================================================

before_dedup = stock_df.count()

stock_df = stock_df.dropDuplicates([
    "product_id",
    "product_name",
    "barcode",
    "store_name",
    "vendor_name",
    "category_name",
    "sub_category_of",
    "brand",
    "quantity",
    "selling_price",
    "printed_mrp",
    "cost_price",
    "total_amount",
    "_store_id",
    "_snapshot_date"
])

after_dedup = stock_df.count()

duplicates_removed = before_dedup - after_dedup

print(
    f"🧹 Duplicate stock rows removed: "
    f"{duplicates_removed:,}"
)

print(
    f"📊 Gold stock rows: "
    f"{after_dedup:,}"
)


# ============================================================
# 7. SNAPSHOT DATE VALIDATION
# ============================================================

wrong_date_count = (
    stock_df
    .filter(
        col("_snapshot_date") != lit(TARGET_DATE)
    )
    .count()
)

if wrong_date_count > 0:
    raise RuntimeError(
        f"❌ {wrong_date_count:,} rows have "
        f"incorrect snapshot date."
    )


# ============================================================
# 8. STORE COVERAGE
# ============================================================

store_count = (
    stock_df
    .select("store_name")
    .distinct()
    .count()
)

print(
    f"🏪 Stores represented in Gold Stock: "
    f"{store_count:,}"
)


# ============================================================
# 9. PRODUCT COVERAGE
# ============================================================

product_count = (
    stock_df
    .select("product_id")
    .filter(
        col("product_id").isNotNull() &
        (col("product_id") != "")
    )
    .distinct()
    .count()
)

print(
    f"📦 Unique products in Gold Stock: "
    f"{product_count:,}"
)


# ============================================================
# 10. IDEMPOTENT WRITE
# ============================================================

if spark.catalog.tableExists(TARGET_TABLE):

    spark.sql(
        f"""
        DELETE FROM {TARGET_TABLE}
        WHERE source_date = '{TARGET_DATE}'
        """
    )

    print(
        f"🗑️ Cleared existing gold_stock "
        f"for {TARGET_DATE}"
    )

else:

    print(
        "ℹ️ gold_stock does not exist yet "
        "— creating it."
    )


# ============================================================
# 11. WRITE GOLD
# ============================================================

(
    stock_df
    .write
    .mode("append")
    .partitionBy("source_date")
    .saveAsTable(TARGET_TABLE)
)


# ============================================================
# 12. POST-WRITE VALIDATION
# ============================================================

written_df = (
    spark.table(TARGET_TABLE)
    .filter(
        col("source_date") == TARGET_DATE
    )
)

written_count = written_df.count()

print(
    f"✅ gold_stock complete: "
    f"{written_count:,} rows"
)

if written_count != after_dedup:
    raise RuntimeError(
        f"❌ Stock write validation failed. "
        f"Expected {after_dedup:,}, "
        f"found {written_count:,}"
    )


# ============================================================
# 13. STORE POST-WRITE VALIDATION
# ============================================================

written_store_count = (
    written_df
    .select("store_name")
    .distinct()
    .count()
)

if written_store_count != store_count:
    raise RuntimeError(
        f"❌ Store coverage changed after Gold write. "
        f"Expected {store_count:,}, "
        f"found {written_store_count:,}"
    )


# ============================================================
# 14. BASIC STOCK TOTALS
# ============================================================

summary = (
    written_df
    .agg(
        spark_sum("quantity").alias("total_qty"),
        spark_sum("total_amount").alias("total_amount")
    )
    .collect()[0]
)

total_qty = summary["total_qty"] or 0
total_amount = summary["total_amount"] or 0

print(
    f"📦 Total stock quantity: "
    f"{total_qty:,.0f}"
)

print(
    f"💰 Total stock amount: "
    f"₹{total_amount:,.2f}"
)


# ============================================================
# FINAL
# ============================================================

print("=" * 70)
print("✅ GOLD STOCK SUCCESS")
print("=" * 70)

# COMMAND ----------

