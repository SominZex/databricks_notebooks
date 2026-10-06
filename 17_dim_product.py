# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# ============================================================
# 17_dim_product
# CANONICAL PRODUCT DIMENSION — BARCODE BASED
#
# Product identity:
#     barcode
#
# Sources:
#     billing_silver
#     stock_silver
#     rtv_silver
#     grn_data
#
# Target:
#     billing.sales_data.dim_product
#
# IMPORTANT:
#     barcode is the canonical product identity.
#
#     productId is retained as a source identifier only.
#
#     Products without a usable barcode are NOT included in
#     dim_product because we cannot safely establish a
#     cross-source product identity for them.
#
# Idempotency:
#     The dimension is rebuilt deterministically from the
#     current source master data.
#
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from datetime import datetime, timezone

# ============================================================
# 1. CONFIG
# ============================================================

CATALOG = "billing"
SCHEMA = "sales_data"

SALES_TABLE = f"{CATALOG}.{SCHEMA}.billing_silver"
STOCK_TABLE = f"{CATALOG}.{SCHEMA}.stock_silver"
RTV_TABLE = f"{CATALOG}.{SCHEMA}.rtv_silver"
GRN_TABLE = f"{CATALOG}.{SCHEMA}.grn_data"

TARGET_TABLE = f"{CATALOG}.{SCHEMA}.dim_product"


print("=" * 80)
print("17 DIM PRODUCT — BARCODE BASED")
print("=" * 80)

print(f"Sales : {SALES_TABLE}")
print(f"Stock : {STOCK_TABLE}")
print(f"RTV   : {RTV_TABLE}")
print(f"GRN   : {GRN_TABLE}")
print(f"Target: {TARGET_TABLE}")


# ============================================================
# 2. CHECK SOURCE TABLES
# ============================================================

required_sources = {
    "sales": SALES_TABLE,
    "stock": STOCK_TABLE,
    "rtv": RTV_TABLE,
    "grn": GRN_TABLE,
}

for source_name, table_name in required_sources.items():

    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(
            f"❌ Required source table does not exist: {table_name}"
        )

    print(f"✅ {source_name}: {table_name}")


# ============================================================
# 3. READ SOURCES
# ============================================================

sales = spark.table(SALES_TABLE)
stock = spark.table(STOCK_TABLE)
rtv = spark.table(RTV_TABLE)
grn = spark.table(GRN_TABLE)


print()
print("SOURCE ROW COUNTS")
print("-" * 80)

print(f"Sales : {sales.count():,}")
print(f"Stock : {stock.count():,}")
print(f"RTV   : {rtv.count():,}")
print(f"GRN   : {grn.count():,}")


# ============================================================
# 4. REQUIRED COLUMN VALIDATION
# ============================================================

sales_required = [
    "productId",
    "productName",
    "barcode",
    "brandName",
    "categoryName",
    "subCategoryOf",
    "description",
    "HSNCode",
    "productMrp",
]

stock_required = [
    "product_id",
    "product_name",
    "barcode",
    "brand",
    "category_name",
    "sub_category_of",
    "printed_mrp",
]

rtv_required = [
    "productId",
    "productName",
    "barcode",
    "vendorName",
]

grn_required = [
    "barcode",
    "productName",
    "brandName",
    "categoryName",
    "sellingPrice",
]


def validate_columns(df, required, source_name):

    missing = [
        c for c in required
        if c not in df.columns
    ]

    if missing:
        raise RuntimeError(
            f"❌ {source_name} missing columns: {missing}"
        )


validate_columns(
    sales,
    sales_required,
    "billing_silver"
)

validate_columns(
    stock,
    stock_required,
    "stock_silver"
)

validate_columns(
    rtv,
    rtv_required,
    "rtv_silver"
)

validate_columns(
    grn,
    grn_required,
    "grn_data"
)

print("✅ Source schema validation passed")


# ============================================================
# 5. CLEAN STRING HELPER
# ============================================================

def clean_string(column_name):

    c = F.trim(
        F.col(column_name).cast(StringType())
    )

    return (
        F.when(
            c.isNull()
            | (c == "")
            | F.lower(c).isin(
                "null",
                "none",
                "nan"
            ),
            F.lit(None)
        )
        .otherwise(c)
    )


# ============================================================
# 6. NORMALIZE BARCODE
# ============================================================
#
# Barcode is the canonical identity.
#
# We:
#   - cast to string
#   - trim whitespace
#   - remove obvious null strings
#
# We DO NOT cast barcode to BIGINT because that can destroy
# leading zeroes.
#
# Example:
#
#     089123456789
#
# must remain:
#
#     "089123456789"
#
# ============================================================

def normalize_barcode(column_name):

    c = clean_string(column_name)

    return (
        F.regexp_replace(
            c,
            r"\s+",
            ""
        )
    )


# ============================================================
# 7. BUILD SALES PRODUCT SOURCE
# ============================================================

sales_products = (
    sales
    .select(
        normalize_barcode("barcode").alias("barcode"),

        clean_string("productId").alias("product_id"),

        clean_string("productName").alias("product_name"),

        clean_string("brandName").alias("brand_name"),

        clean_string("categoryName").alias("category_name"),

        clean_string("subCategoryOf").alias("sub_category"),

        clean_string("description").alias("description"),

        clean_string("HSNCode").alias("hsn_code"),

        F.col("productMrp")
        .cast("double")
        .alias("mrp"),

        F.lit("sales").alias("source_system")
    )
)


# ============================================================
# 8. BUILD STOCK PRODUCT SOURCE
# ============================================================

stock_products = (
    stock
    .select(
        normalize_barcode("barcode").alias("barcode"),

        clean_string("product_id").alias("product_id"),

        clean_string("product_name").alias("product_name"),

        clean_string("brand").alias("brand_name"),

        clean_string("category_name").alias("category_name"),

        clean_string("sub_category_of").alias("sub_category"),

        F.lit(None)
        .cast("string")
        .alias("description"),

        F.lit(None)
        .cast("string")
        .alias("hsn_code"),

        F.col("printed_mrp")
        .cast("double")
        .alias("mrp"),

        F.lit("stock").alias("source_system")
    )
)


# ============================================================
# 9. BUILD RTV PRODUCT SOURCE
# ============================================================

rtv_products = (
    rtv
    .select(
        normalize_barcode("barcode").alias("barcode"),

        clean_string("productId").alias("product_id"),

        clean_string("productName").alias("product_name"),

        F.lit(None)
        .cast("string")
        .alias("brand_name"),

        F.lit(None)
        .cast("string")
        .alias("category_name"),

        F.lit(None)
        .cast("string")
        .alias("sub_category"),

        F.lit(None)
        .cast("string")
        .alias("description"),

        F.lit(None)
        .cast("string")
        .alias("hsn_code"),

        F.lit(None)
        .cast("double")
        .alias("mrp"),

        F.lit("rtv").alias("source_system")
    )
)


# ============================================================
# 10. BUILD GRN PRODUCT SOURCE
# ============================================================

grn_products = (
    grn
    .select(
        normalize_barcode("barcode").alias("barcode"),

        F.lit(None)
        .cast("string")
        .alias("product_id"),

        clean_string("productName").alias("product_name"),

        clean_string("brandName").alias("brand_name"),

        clean_string("categoryName").alias("category_name"),

        F.lit(None)
        .cast("string")
        .alias("sub_category"),

        F.lit(None)
        .cast("string")
        .alias("description"),

        F.lit(None)
        .cast("string")
        .alias("hsn_code"),

        F.col("sellingPrice")
        .cast("double")
        .alias("mrp"),

        F.lit("grn").alias("source_system")
    )
)


# ============================================================
# 11. UNION ALL SOURCES
# ============================================================

all_products = (
    sales_products
    .unionByName(stock_products)
    .unionByName(rtv_products)
    .unionByName(grn_products)
)


combined_count = all_products.count()

print()
print(
    f"Combined product-source rows: {combined_count:,}"
)


# ============================================================
# 12. BARCODE VALIDATION
# ============================================================
#
# Product identity requires barcode.
#
# Invalid/missing barcodes are excluded from the canonical
# product dimension.
# ============================================================

missing_barcode_count = (
    all_products
    .filter(
        F.col("barcode").isNull()
        |
        (F.length(F.trim(F.col("barcode"))) == 0)
    )
    .count()
)

print()
print(
    f"Rows without usable barcode: "
    f"{missing_barcode_count:,}"
)


all_products = (
    all_products
    .filter(
        F.col("barcode").isNotNull()
        &
        (F.length(F.trim(F.col("barcode"))) > 0)
    )
)


if all_products.count() == 0:
    raise RuntimeError(
        "❌ No products with usable barcodes were found."
    )


# ============================================================
# 13. BARCODE FORMAT NORMALIZATION
# ============================================================
#
# Some source systems may represent a numeric barcode as:
#
#     890123456789
#
# while another may represent it as:
#
#     890123456789.0
#
# Remove ONLY a trailing ".0" when the complete value is
# otherwise numeric.
#
# We do NOT remove meaningful characters from the barcode.
# ============================================================

all_products = (
    all_products
    .withColumn(
        "barcode",
        F.when(
            F.col("barcode").rlike(r"^\d+\.0$"),
            F.regexp_replace(
                F.col("barcode"),
                r"\.0$",
                ""
            )
        )
        .otherwise(
            F.col("barcode")
        )
    )
)


# ============================================================
# 14. BARCODE → PRODUCT ID RECONCILIATION
# ============================================================
#
# This is now a diagnostic only.
#
# Multiple product IDs under one barcode are a source-data
# inconsistency, but they MUST NOT create multiple products.
# ============================================================

barcode_product_id_conflicts = (
    all_products
    .filter(
        F.col("product_id").isNotNull()
    )
    .groupBy("barcode")
    .agg(
        F.countDistinct("product_id")
        .alias("product_id_count")
    )
    .filter(
        F.col("product_id_count") > 1
    )
)

barcode_product_id_conflict_count = (
    barcode_product_id_conflicts.count()
)


print()
print("BARCODE IDENTITY RECONCILIATION")
print("-" * 80)

print(
    f"Barcodes mapped to multiple product IDs: "
    f"{barcode_product_id_conflict_count:,}"
)

if barcode_product_id_conflict_count > 0:

    print(
        "⚠️ Source-system product ID conflicts detected."
    )

    display(
        barcode_product_id_conflicts
        .orderBy(
            F.desc("product_id_count")
        )
        .limit(100)
    )


# ============================================================
# 15. BARCODE → PRODUCT NAME RECONCILIATION
# ============================================================

barcode_name_conflicts = (
    all_products
    .filter(
        F.col("product_name").isNotNull()
    )
    .withColumn(
        "_normalized_name",
        F.lower(
            F.regexp_replace(
                F.trim(
                    F.col("product_name")
                ),
                r"\s+",
                " "
            )
        )
    )
    .groupBy("barcode")
    .agg(
        F.countDistinct(
            "_normalized_name"
        ).alias("product_name_count")
    )
    .filter(
        F.col("product_name_count") > 1
    )
)

barcode_name_conflict_count = (
    barcode_name_conflicts.count()
)

print(
    f"Barcodes mapped to multiple product names: "
    f"{barcode_name_conflict_count:,}"
)

if barcode_name_conflict_count > 0:

    print(
        "⚠️ Product-name variations exist. "
        "They will be consolidated under the barcode."
    )


# ============================================================
# 16. CREATE PRODUCT KEY
# ============================================================
#
# FINAL IDENTITY:
#
#     product_key = SHA256(barcode)
#
# Same barcode across Sales/Stock/RTV/GRN
# ALWAYS maps to the same product_key.
# ============================================================

product_dim = (
    all_products
    .withColumn(
        "product_key",
        F.sha2(
            F.concat(
                F.lit("product:barcode:"),
                F.col("barcode")
            ),
            256
        )
    )
)


# ============================================================
# 17. COLLAPSE ALL SOURCE RECORDS
# ============================================================
#
# Multiple records for one barcode become ONE product.
#
# first(..., ignorenulls=True) ensures that if one source has
# missing metadata and another has it, we retain the populated
# value.
#
# For conflicting metadata, the source data is consolidated
# rather than creating duplicate products.
# ============================================================

product_dim = (
    product_dim
    .groupBy(
        "product_key",
        "barcode"
    )
    .agg(

        F.first(
            "product_id",
            ignorenulls=True
        ).alias("product_id"),

        F.first(
            "product_name",
            ignorenulls=True
        ).alias("product_name"),

        F.first(
            "brand_name",
            ignorenulls=True
        ).alias("brand_name"),

        F.first(
            "category_name",
            ignorenulls=True
        ).alias("category_name"),

        F.first(
            "sub_category",
            ignorenulls=True
        ).alias("sub_category"),

        F.first(
            "description",
            ignorenulls=True
        ).alias("description"),

        F.first(
            "hsn_code",
            ignorenulls=True
        ).alias("hsn_code"),

        F.max(
            "mrp"
        ).alias("mrp"),

        F.collect_set(
            "source_system"
        ).alias("_source_systems")
    )
)


# ============================================================
# 18. PRODUCT NAME FALLBACK
# ============================================================

product_dim = (
    product_dim
    .withColumn(
        "product_name",
        F.when(
            F.col("product_name").isNull()
            |
            (
                F.length(
                    F.trim(
                        F.col("product_name")
                    )
                ) == 0
            ),
            F.lit("UNKNOWN PRODUCT")
        )
        .otherwise(
            F.trim(
                F.col("product_name")
            )
        )
    )
)


# ============================================================
# 19. ACTIVE FLAG
# ============================================================

product_dim = (
    product_dim
    .withColumn(
        "is_active",
        F.lit(True)
    )
)


# ============================================================
# 20. TIMESTAMPS
# ============================================================

now_utc = datetime.now(timezone.utc)

product_dim = (
    product_dim
    .withColumn(
        "created_at",
        F.lit(now_utc)
        .cast("timestamp")
    )
    .withColumn(
        "updated_at",
        F.lit(now_utc)
        .cast("timestamp")
    )
)


# ============================================================
# 21. FINAL COLUMN ORDER
# ============================================================

product_dim = (
    product_dim
    .select(
        "product_key",
        "barcode",
        "product_id",
        "product_name",
        "brand_name",
        "category_name",
        "sub_category",
        "description",
        "hsn_code",
        "mrp",
        "is_active",
        "created_at",
        "updated_at"
    )
)


# ============================================================
# 22. PRE-WRITE VALIDATION
# ============================================================

prewrite_count = product_dim.count()

if prewrite_count == 0:
    raise RuntimeError(
        "❌ Product dimension contains 0 records."
    )


null_key_count = (
    product_dim
    .filter(
        F.col("product_key").isNull()
    )
    .count()
)

if null_key_count > 0:
    raise RuntimeError(
        f"❌ {null_key_count} NULL product keys."
    )


null_barcode_count = (
    product_dim
    .filter(
        F.col("barcode").isNull()
        |
        (F.length(F.trim(F.col("barcode"))) == 0)
    )
    .count()
)

if null_barcode_count > 0:
    raise RuntimeError(
        f"❌ {null_barcode_count} NULL/empty barcodes."
    )


duplicate_key_count = (
    product_dim
    .groupBy("product_key")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

if duplicate_key_count > 0:
    raise RuntimeError(
        f"❌ {duplicate_key_count} duplicate product keys."
    )


duplicate_barcode_count = (
    product_dim
    .groupBy("barcode")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

if duplicate_barcode_count > 0:
    raise RuntimeError(
        f"❌ {duplicate_barcode_count} duplicate barcodes."
    )


print()
print("PRE-WRITE VALIDATION")
print("-" * 80)

print(
    f"Canonical products : {prewrite_count:,}"
)

print("✅ product_key NULL validation passed")
print("✅ barcode NULL validation passed")
print("✅ product_key uniqueness validation passed")
print("✅ barcode uniqueness validation passed")


# ============================================================
# 23. DROP OLD INCORRECT DIMENSION
# ============================================================
#
# The previous dim_product used product_id/barcode/name
# identity and therefore must NOT be merged into this new
# barcode-based model.
#
# Since this is the initial semantic dimension and downstream
# dimensions have not yet been built, recreate it cleanly.
# ============================================================

if spark.catalog.tableExists(TARGET_TABLE):

    print()
    print(
        f"⚠️ Existing {TARGET_TABLE} detected."
    )

    print(
        "Dropping previous product dimension so the "
        "barcode-based identity can be created cleanly."
    )

    spark.sql(
        f"DROP TABLE {TARGET_TABLE}"
    )

    print(
        "✅ Previous dim_product removed."
    )


# ============================================================
# 24. CREATE NEW DIMENSION
# ============================================================

print()
print(
    "Creating barcode-based dim_product..."
)

(
    product_dim
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

print(
    "✅ dim_product created successfully."
)


# ============================================================
# 25. POST-WRITE VALIDATION
# ============================================================

final_df = spark.table(TARGET_TABLE)

final_count = final_df.count()

print()
print("=" * 80)
print("POST-WRITE VALIDATION")
print("=" * 80)

print(
    f"Total products in dim_product: "
    f"{final_count:,}"
)


# ============================================================
# 26. FINAL KEY VALIDATION
# ============================================================

final_null_keys = (
    final_df
    .filter(
        F.col("product_key").isNull()
    )
    .count()
)

if final_null_keys > 0:
    raise RuntimeError(
        f"❌ Final table contains "
        f"{final_null_keys} NULL product keys."
    )


final_duplicate_keys = (
    final_df
    .groupBy("product_key")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

if final_duplicate_keys > 0:
    raise RuntimeError(
        f"❌ Final table contains "
        f"{final_duplicate_keys} duplicate product keys."
    )


final_duplicate_barcodes = (
    final_df
    .groupBy("barcode")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

if final_duplicate_barcodes > 0:
    raise RuntimeError(
        f"❌ Final table contains "
        f"{final_duplicate_barcodes} duplicate barcodes."
    )


print("✅ Final product_key validation passed")
print("✅ Final barcode uniqueness validation passed")


# ============================================================
# 27. FINAL COVERAGE
# ============================================================

print()
print("FINAL PRODUCT COVERAGE")
print("-" * 80)

print(
    f"Products             : {final_count:,}"
)

print(
    f"With product_id      : "
    f"{final_df.filter(F.col('product_id').isNotNull()).count():,}"
)

print(
    f"With brand           : "
    f"{final_df.filter(F.col('brand_name').isNotNull()).count():,}"
)

print(
    f"With category        : "
    f"{final_df.filter(F.col('category_name').isNotNull()).count():,}"
)

print(
    f"With sub-category    : "
    f"{final_df.filter(F.col('sub_category').isNotNull()).count():,}"
)


# ============================================================
# 28. SAMPLE
# ============================================================

display(
    final_df
    .select(
        "product_key",
        "barcode",
        "product_id",
        "product_name",
        "brand_name",
        "category_name",
        "sub_category",
        "mrp",
        "is_active"
    )
    .orderBy("product_name")
    .limit(100)
)


# ============================================================
# 29. FINAL SUCCESS
# ============================================================

print()
print("=" * 80)
print("17 DIM PRODUCT SUCCESS")
print("=" * 80)

print(
    f"Canonical products : {final_count:,}"
)

print(
    f"Product identity   : BARCODE"
)

print(
    f"Target             : {TARGET_TABLE}"
)

print("=" * 80)

# COMMAND ----------

