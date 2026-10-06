# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    trim,
    lit,
    sum as spark_sum
)

from pyspark.sql.types import DecimalType


# ============================================================
# GOLD GRN
# ============================================================

SOURCE_TABLE = f"{catalog_name}.{schema_name}.grn_data"
TARGET_TABLE = f"{catalog_name}.{schema_name}.gold_grn"

print("=" * 70)
print("GOLD GRN")
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

grn = spark.table(SOURCE_TABLE)

total_source_rows = grn.count()

print(
    f"📥 GRN source rows: "
    f"{total_source_rows:,}"
)

if total_source_rows == 0:
    raise RuntimeError(
        "❌ GRN source table is empty."
    )


# ============================================================
# 2. REQUIRED COLUMNS
# ============================================================

required_columns = [
    "Date",
    "Time",
    "acess",
    "acessAmount",
    "amountNet",
    "barcode",
    "brandName",
    "categoryName",
    "cess",
    "cessAmount",
    "costPrice",
    "grnId",
    "gst",
    "gstAmount",
    "invoiceNumber",
    "productName",
    "recievedQuantity",
    "sellingPrice",
    "storeName",
    "totalCost",
    "totalTaxes",
    "user",
    "vendor",
    "vendorGst"
]

missing_columns = [
    c for c in required_columns
    if c not in grn.columns
]

if missing_columns:
    raise RuntimeError(
        f"❌ Missing required GRN columns: "
        f"{missing_columns}"
    )


# ============================================================
# 3. FILTER TARGET DATE
# ============================================================

grn_df = (
    grn
    .filter(
        col("Date") == lit(TARGET_DATE)
    )
)

source_date_count = grn_df.count()

print(
    f"📅 GRN rows for {TARGET_DATE}: "
    f"{source_date_count:,}"
)

if source_date_count == 0:
    raise RuntimeError(
        f"❌ No GRN data found for Date={TARGET_DATE}"
    )


# ============================================================
# 4. NORMALIZE / CLEAN
# ============================================================

grn_df = (
    grn_df

    # --------------------------------------------------------
    # String columns
    # --------------------------------------------------------

    .withColumn(
        "grnId",
        trim(col("grnId").cast("string"))
    )

    .withColumn(
        "barcode",
        trim(col("barcode").cast("string"))
    )

    .withColumn(
        "productName",
        trim(col("productName").cast("string"))
    )

    .withColumn(
        "storeName",
        trim(col("storeName").cast("string"))
    )

    .withColumn(
        "vendor",
        trim(col("vendor").cast("string"))
    )

    .withColumn(
        "vendorGst",
        trim(col("vendorGst").cast("string"))
    )

    .withColumn(
        "brandName",
        trim(col("brandName").cast("string"))
    )

    .withColumn(
        "categoryName",
        trim(col("categoryName").cast("string"))
    )

    .withColumn(
        "user",
        trim(col("user").cast("string"))
    )

    .withColumn(
        "invoiceNumber",
        trim(col("invoiceNumber").cast("string"))
    )

    .withColumn(
        "Time",
        trim(col("Time").cast("string"))
    )

    # --------------------------------------------------------
    # Numeric columns
    # --------------------------------------------------------

    .withColumn(
        "recievedQuantity",
        col("recievedQuantity").cast("bigint")
    )

    .withColumn(
        "sellingPrice",
        col("sellingPrice").cast("decimal(18,6)")
    )

    .withColumn(
        "costPrice",
        col("costPrice").cast("decimal(18,6)")
    )

    .withColumn(
        "totalCost",
        col("totalCost").cast("decimal(18,6)")
    )

    .withColumn(
        "gst",
        col("gst").cast("decimal(18,6)")
    )

    .withColumn(
        "gstAmount",
        col("gstAmount").cast("decimal(18,6)")
    )

    .withColumn(
        "cess",
        col("cess").cast("decimal(18,6)")
    )

    .withColumn(
        "cessAmount",
        col("cessAmount").cast("decimal(18,6)")
    )

    .withColumn(
        "acess",
        col("acess").cast("decimal(18,6)")
    )

    .withColumn(
        "acessAmount",
        col("acessAmount").cast("decimal(18,6)")
    )

    .withColumn(
        "totalTaxes",
        col("totalTaxes").cast("decimal(18,6)")
    )

    .withColumn(
        "amountNet",
        col("amountNet").cast("decimal(18,6)")
    )

    # --------------------------------------------------------
    # Gold metadata
    # --------------------------------------------------------

    .withColumn(
        "source_date",
        col("Date")
    )
)


# ============================================================
# 5. VALIDATION
# ============================================================

# ------------------------------------------------------------
# Date validation
# ------------------------------------------------------------

null_date_count = (
    grn_df
    .filter(col("Date").isNull())
    .count()
)

if null_date_count > 0:
    raise RuntimeError(
        f"❌ {null_date_count:,} GRN rows have NULL Date."
    )


# ------------------------------------------------------------
# GRN ID validation
# ------------------------------------------------------------

null_grnid_count = (
    grn_df
    .filter(
        col("grnId").isNull() |
        (col("grnId") == "")
    )
    .count()
)

if null_grnid_count > 0:
    raise RuntimeError(
        f"❌ {null_grnid_count:,} GRN rows have "
        f"NULL/empty grnId."
    )


# ------------------------------------------------------------
# Store validation
# ------------------------------------------------------------

null_store_count = (
    grn_df
    .filter(
        col("storeName").isNull() |
        (col("storeName") == "")
    )
    .count()
)

if null_store_count > 0:
    raise RuntimeError(
        f"❌ {null_store_count:,} GRN rows have "
        f"NULL/empty storeName."
    )


# ------------------------------------------------------------
# Quantity validation
# ------------------------------------------------------------

null_quantity_count = (
    grn_df
    .filter(col("recievedQuantity").isNull())
    .count()
)

if null_quantity_count > 0:
    print(
        f"⚠️ Warning: {null_quantity_count:,} rows have "
        f"NULL recievedQuantity."
    )


# ============================================================
# 6. REMOVE EXACT DUPLICATES
# ============================================================
#
# We remove only exact duplicate GRN records.
#
# We do NOT aggregate quantities or costs here.
#
# ============================================================

before_dedup = grn_df.count()

grn_df = grn_df.dropDuplicates([
    "Date",
    "Time",
    "grnId",
    "barcode",
    "productName",
    "recievedQuantity",
    "sellingPrice",
    "costPrice",
    "totalCost",
    "storeName",
    "vendor",
    "invoiceNumber"
])

after_dedup = grn_df.count()

duplicates_removed = (
    before_dedup - after_dedup
)

print(
    f"🧹 Duplicate GRN rows removed: "
    f"{duplicates_removed:,}"
)

print(
    f"📊 Gold GRN rows: "
    f"{after_dedup:,}"
)


# ============================================================
# 7. STORE COVERAGE
# ============================================================

store_count = (
    grn_df
    .select("storeName")
    .distinct()
    .count()
)

print(
    f"🏪 Stores represented in Gold GRN: "
    f"{store_count:,}"
)


# ============================================================
# 8. VENDOR COVERAGE
# ============================================================

vendor_count = (
    grn_df
    .select("vendor")
    .filter(
        col("vendor").isNotNull() &
        (col("vendor") != "")
    )
    .distinct()
    .count()
)

print(
    f"🏢 Vendors represented in Gold GRN: "
    f"{vendor_count:,}"
)


# ============================================================
# 9. IDEMPOTENT WRITE
# ============================================================

if spark.catalog.tableExists(TARGET_TABLE):

    spark.sql(
        f"""
        DELETE FROM {TARGET_TABLE}
        WHERE source_date = '{TARGET_DATE}'
        """
    )

    print(
        f"🗑️ Cleared existing gold_grn "
        f"for {TARGET_DATE}"
    )

else:

    print(
        "ℹ️ gold_grn does not exist yet "
        "— creating it."
    )


# ============================================================
# 10. WRITE GOLD
# ============================================================

(
    grn_df
    .write
    .format("delta")
    .mode("append")
    .partitionBy("source_date")
    .saveAsTable(TARGET_TABLE)
)


# ============================================================
# 11. POST-WRITE VALIDATION
# ============================================================

written_df = (
    spark.table(TARGET_TABLE)
    .filter(
        col("source_date") == TARGET_DATE
    )
)

written_count = written_df.count()

print(
    f"✅ gold_grn complete: "
    f"{written_count:,} rows"
)

if written_count != after_dedup:
    raise RuntimeError(
        f"❌ GRN write validation failed. "
        f"Expected {after_dedup:,}, "
        f"found {written_count:,}"
    )


# ============================================================
# 12. POST-WRITE STORE VALIDATION
# ============================================================

written_store_count = (
    written_df
    .select("storeName")
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
# 13. BUSINESS TOTALS
# ============================================================

summary = (
    written_df
    .agg(
        spark_sum("recievedQuantity")
            .alias("total_received_qty"),

        spark_sum("totalCost")
            .alias("total_purchase_cost"),

        spark_sum("amountNet")
            .alias("total_net_amount"),

        spark_sum("totalTaxes")
            .alias("total_taxes")
    )
    .collect()[0]
)

total_received_qty = (
    summary["total_received_qty"] or 0
)

total_purchase_cost = (
    summary["total_purchase_cost"] or 0
)

total_net_amount = (
    summary["total_net_amount"] or 0
)

total_taxes = (
    summary["total_taxes"] or 0
)

print(
    f"📦 Total received quantity: "
    f"{total_received_qty:,.0f}"
)

print(
    f"💰 Total purchase cost: "
    f"₹{float(total_purchase_cost):,.2f}"
)

print(
    f"💰 Total net amount: "
    f"₹{float(total_net_amount):,.2f}"
)

print(
    f"🧾 Total taxes: "
    f"₹{float(total_taxes):,.2f}"
)


# ============================================================
# FINAL
# ============================================================

print("=" * 70)
print("✅ GOLD GRN SUCCESS")
print("=" * 70)

# COMMAND ----------

