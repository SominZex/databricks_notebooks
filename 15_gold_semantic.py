# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    trim,
    lower,
    lit,
    round as spark_round,
    sum as spark_sum
)


# ============================================================
# 15 GOLD SEMANTIC — FACT SALES
# ============================================================

SOURCE_TABLE = f"{catalog_name}.{schema_name}.billing_silver"
TARGET_TABLE = f"{catalog_name}.{schema_name}.fact_sales"

print("=" * 70)
print("15 GOLD SEMANTIC — FACT SALES")
print("=" * 70)
print(f"Source table : {SOURCE_TABLE}")
print(f"Target table : {TARGET_TABLE}")
print(f"Target date  : {TARGET_DATE}")
print("=" * 70)


# ============================================================
# EXCLUDED STORES
# ============================================================

EXCLUDED_STORES = [
    "ho marlboro",
    "dummy store --- for testing only"
]

print("Excluded stores:")
for store in EXCLUDED_STORES:
    print(f"  - {store}")

print("=" * 70)


# ============================================================
# 1. SOURCE VALIDATION
# ============================================================

if not spark.catalog.tableExists(SOURCE_TABLE):
    raise RuntimeError(
        f"❌ Source table does not exist: {SOURCE_TABLE}"
    )

silver = spark.table(SOURCE_TABLE)

print(
    f"📥 Silver rows: {silver.count():,}"
)


# ============================================================
# 2. REQUIRED COLUMNS
# ============================================================

required_columns = [
    "invoice",
    "storeInvoice",
    "orderDate",
    "time",
    "productId",
    "productName",
    "barcode",
    "quantity",
    "productMrp",
    "sellingPrice",
    "discountAmount",
    "totalProductPrice",
    "deliveryFee",
    "HSNCode",
    "GST",
    "GSTAmount",
    "CGSTRate",
    "CGSTAmount",
    "SGSTRate",
    "SGSTAmount",
    "acessAmount",
    "cess",
    "cessAmount",
    "orderAmountTax",
    "orderAmountNet",
    "cashAmount",
    "cardAmount",
    "upiAmount",
    "creditAmount",
    "costPrice",
    "description",
    "brandName",
    "categoryName",
    "subCategoryOf",
    "storeName",
    "GSTIN",
    "orderType",
    "paymentMethod",
    "customerName",
    "customerNumber",
    "orderFrom",
    "orderStatus"
]

missing_columns = [
    c for c in required_columns
    if c not in silver.columns
]

if missing_columns:
    raise RuntimeError(
        f"❌ Missing required billing columns: "
        f"{missing_columns}"
    )


# ============================================================
# 3. FILTER TARGET DATE
# ============================================================

sales = (
    silver
    .filter(
        col("orderDate") == lit(TARGET_DATE)
    )
)

source_count = sales.count()

print(
    f"📅 Sales rows for {TARGET_DATE}: "
    f"{source_count:,}"
)

if source_count == 0:
    raise RuntimeError(
        f"❌ No sales data found for {TARGET_DATE}"
    )


# ============================================================
# 4. BUSINESS RULE
# ============================================================

# IMPORTANT:
#
# We intentionally DO NOT filter:
#
#     orderStatus == 'PLACED'
#
# Your reporting aggregate does not use that filter.
#
# Excluded stores:
#
#     1. Ho Marlboro
#     2. Dummy Store --- For Testing Only
#
# Comparison is case-insensitive and whitespace-trimmed.
#
# ============================================================

sales = (
    sales
    .filter(
        ~lower(trim(col("storeName"))).isin(EXCLUDED_STORES)
    )
)

after_store_exclusion = sales.count()

print(
    f"🏪 Rows after excluded-store filtering: "
    f"{after_store_exclusion:,}"
)

if after_store_exclusion == 0:
    raise RuntimeError(
        f"❌ All sales rows were removed by EXCLUDED_STORES "
        f"for {TARGET_DATE}"
    )


# ============================================================
# 5. NORMALIZE DIMENSIONAL COLUMNS
# ============================================================

sales = (
    sales

    # --------------------------------------------------------
    # Identifiers
    # --------------------------------------------------------

    .withColumn(
        "invoice",
        trim(col("invoice").cast("string"))
    )

    .withColumn(
        "storeInvoice",
        trim(col("storeInvoice").cast("string"))
    )

    .withColumn(
        "productId",
        col("productId").cast("long")
    )

    .withColumn(
        "barcode",
        col("barcode").cast("long")
    )

    # --------------------------------------------------------
    # Product
    # --------------------------------------------------------

    .withColumn(
        "productName",
        trim(col("productName").cast("string"))
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
        "subCategoryOf",
        trim(col("subCategoryOf").cast("string"))
    )

    .withColumn(
        "HSNCode",
        trim(col("HSNCode").cast("string"))
    )

    # --------------------------------------------------------
    # Store
    # --------------------------------------------------------

    .withColumn(
        "storeName",
        trim(col("storeName").cast("string"))
    )

    .withColumn(
        "GSTIN",
        trim(col("GSTIN").cast("string"))
    )

    # --------------------------------------------------------
    # Customer
    # --------------------------------------------------------

    .withColumn(
        "customerName",
        trim(col("customerName").cast("string"))
    )

    .withColumn(
        "customerNumber",
        trim(col("customerNumber").cast("string"))
    )

    # --------------------------------------------------------
    # Other dimensions
    # --------------------------------------------------------

    .withColumn(
        "orderType",
        trim(col("orderType").cast("string"))
    )

    .withColumn(
        "paymentMethod",
        trim(col("paymentMethod").cast("string"))
    )

    .withColumn(
        "orderFrom",
        trim(col("orderFrom").cast("string"))
    )

    .withColumn(
        "orderStatus",
        trim(col("orderStatus").cast("string"))
    )

    # --------------------------------------------------------
    # Numeric fields
    # --------------------------------------------------------

    .withColumn(
        "quantity",
        col("quantity").cast("int")
    )

    .withColumn(
        "productMrp",
        col("productMrp").cast("double")
    )

    .withColumn(
        "sellingPrice",
        col("sellingPrice").cast("double")
    )

    .withColumn(
        "discountAmount",
        col("discountAmount").cast("double")
    )

    .withColumn(
        "totalProductPrice",
        col("totalProductPrice").cast("double")
    )

    .withColumn(
        "deliveryFee",
        col("deliveryFee").cast("double")
    )

    .withColumn(
        "GST",
        col("GST").cast("bigint")
    )

    .withColumn(
        "GSTAmount",
        col("GSTAmount").cast("double")
    )

    .withColumn(
        "CGSTRate",
        col("CGSTRate").cast("double")
    )

    .withColumn(
        "CGSTAmount",
        col("CGSTAmount").cast("double")
    )

    .withColumn(
        "SGSTRate",
        col("SGSTRate").cast("double")
    )

    .withColumn(
        "SGSTAmount",
        col("SGSTAmount").cast("double")
    )

    .withColumn(
        "acessAmount",
        col("acessAmount").cast("double")
    )

    .withColumn(
        "cess",
        col("cess").cast("double")
    )

    .withColumn(
        "cessAmount",
        col("cessAmount").cast("double")
    )

    .withColumn(
        "orderAmountTax",
        col("orderAmountTax").cast("double")
    )

    .withColumn(
        "orderAmountNet",
        col("orderAmountNet").cast("double")
    )

    .withColumn(
        "cashAmount",
        col("cashAmount").cast("double")
    )

    .withColumn(
        "cardAmount",
        col("cardAmount").cast("double")
    )

    .withColumn(
        "upiAmount",
        col("upiAmount").cast("double")
    )

    .withColumn(
        "creditAmount",
        col("creditAmount").cast("double")
    )

    .withColumn(
        "costPrice",
        col("costPrice").cast("double")
    )

    # --------------------------------------------------------
    # Semantic metadata
    # --------------------------------------------------------

    .withColumn(
        "source_date",
        col("orderDate")
    )
)


# ============================================================
# 6. DATA VALIDATION
# ============================================================

# ------------------------------------------------------------
# Date
# ------------------------------------------------------------

null_date_count = (
    sales
    .filter(col("orderDate").isNull())
    .count()
)

if null_date_count > 0:
    raise RuntimeError(
        f"❌ {null_date_count:,} rows have NULL orderDate."
    )


# ------------------------------------------------------------
# Invoice
# ------------------------------------------------------------

null_invoice_count = (
    sales
    .filter(
        col("invoice").isNull() |
        (col("invoice") == "")
    )
    .count()
)

if null_invoice_count > 0:
    raise RuntimeError(
        f"❌ {null_invoice_count:,} rows have "
        f"NULL/empty invoice."
    )


# ------------------------------------------------------------
# Store
# ------------------------------------------------------------

null_store_count = (
    sales
    .filter(
        col("storeName").isNull() |
        (col("storeName") == "")
    )
    .count()
)

if null_store_count > 0:
    raise RuntimeError(
        f"❌ {null_store_count:,} rows have "
        f"NULL/empty storeName."
    )


# ------------------------------------------------------------
# Verify excluded stores are actually absent
# ------------------------------------------------------------

remaining_excluded_rows = (
    sales
    .filter(
        lower(trim(col("storeName"))).isin(EXCLUDED_STORES)
    )
    .count()
)

if remaining_excluded_rows > 0:
    raise RuntimeError(
        f"❌ Excluded stores still exist in fact_sales dataset: "
        f"{remaining_excluded_rows:,} rows"
    )


print(
    "✅ Excluded-store validation passed."
)


# ============================================================
# 7. DUPLICATE CHECK
# ============================================================

# IMPORTANT:
#
# We DO NOT drop duplicate rows blindly here.
#
# A single invoice can legitimately contain multiple identical
# product lines.
#
# Therefore transaction-line grain must be preserved.
#
# ============================================================

duplicate_invoice_count = (
    sales
    .groupBy("invoice")
    .count()
    .filter(col("count") > 1)
    .count()
)

print(
    f"🧾 Invoices containing multiple sales lines: "
    f"{duplicate_invoice_count:,}"
)


# ============================================================
# 8. FINAL COLUMN ORDER
# ============================================================

fact_sales = sales.select(
    "invoice",
    "storeInvoice",
    "orderDate",
    "time",

    "productId",
    "productName",
    "barcode",

    "quantity",
    "productMrp",
    "sellingPrice",
    "discountAmount",
    "totalProductPrice",
    "deliveryFee",

    "HSNCode",
    "GST",
    "GSTAmount",
    "CGSTRate",
    "CGSTAmount",
    "SGSTRate",
    "SGSTAmount",
    "acessAmount",
    "cess",
    "cessAmount",

    "orderAmountTax",
    "orderAmountNet",

    "cashAmount",
    "cardAmount",
    "upiAmount",
    "creditAmount",

    "costPrice",

    "description",

    "brandName",
    "categoryName",
    "subCategoryOf",

    "storeName",
    "GSTIN",

    "orderType",
    "paymentMethod",
    "customerName",
    "customerNumber",
    "orderFrom",
    "orderStatus",

    "source_date"
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
        f"🗑️ Cleared existing fact_sales "
        f"for {TARGET_DATE}"
    )

else:

    print(
        "ℹ️ fact_sales does not exist yet "
        "— creating it."
    )


# ============================================================
# 10. WRITE
# ============================================================

(
    fact_sales
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
    f"✅ fact_sales rows written: "
    f"{written_count:,}"
)

expected_count = fact_sales.count()

if written_count != expected_count:
    raise RuntimeError(
        f"❌ fact_sales write validation failed. "
        f"Expected {expected_count:,}, "
        f"found {written_count:,}"
    )


# ============================================================
# 12. BUSINESS TOTALS
# ============================================================

summary = (
    written_df
    .agg(
        spark_round(
            spark_sum("quantity"),
            2
        ).alias("total_quantity"),

        spark_round(
            spark_sum("totalProductPrice"),
            2
        ).alias("total_sales"),

        spark_round(
            spark_sum("discountAmount"),
            2
        ).alias("total_discount")
    )
    .collect()[0]
)

total_quantity = summary["total_quantity"] or 0
total_sales = summary["total_sales"] or 0
total_discount = summary["total_discount"] or 0


# ============================================================
# 13. COVERAGE
# ============================================================

invoice_count = (
    written_df
    .select("invoice")
    .distinct()
    .count()
)

store_count = (
    written_df
    .select("storeName")
    .distinct()
    .count()
)

product_count = (
    written_df
    .select("productId")
    .filter(col("productId").isNotNull())
    .distinct()
    .count()
)

brand_count = (
    written_df
    .select("brandName")
    .filter(
        col("brandName").isNotNull() &
        (col("brandName") != "")
    )
    .distinct()
    .count()
)


# ============================================================
# 14. FINAL SUMMARY
# ============================================================

print("=" * 70)
print("FACT SALES SUMMARY")
print("=" * 70)

print(
    f"🧾 Unique invoices : {invoice_count:,}"
)

print(
    f"🏪 Stores          : {store_count:,}"
)

print(
    f"📦 Products        : {product_count:,}"
)

print(
    f"🏷️ Brands          : {brand_count:,}"
)

print(
    f"📊 Quantity        : {total_quantity:,.0f}"
)

print(
    f"💰 Sales           : ₹{total_sales:,.2f}"
)

print(
    f"🏷️ Discounts       : ₹{total_discount:,.2f}"
)

print("=" * 70)
print("Excluded stores:")
for store in EXCLUDED_STORES:
    print(f"  - {store}")

print("=" * 70)
print("✅ FACT SALES SUCCESS")
print("=" * 70)

# COMMAND ----------

