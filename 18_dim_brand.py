# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# ============================================================
# 18_dim_brand
# CANONICAL BRAND DIMENSION
#
# Source:
#   billing.sales_data.dim_product
#
# Target:
#   billing.sales_data.dim_brand
#
# Grain:
#   1 row per normalized brand
#
# Product identity is NOT recreated here.
# dim_product is the canonical product source.
# ============================================================

from pyspark.sql import functions as F
from datetime import datetime, timezone


# ============================================================
# 1. CONFIG
# ============================================================

CATALOG = "billing"
SCHEMA = "sales_data"

SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.dim_product"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.dim_brand"


print("=" * 80)
print("18 DIM BRAND")
print("=" * 80)

print(f"Source : {SOURCE_TABLE}")
print(f"Target : {TARGET_TABLE}")


# ============================================================
# 2. CHECK SOURCE
# ============================================================

if not spark.catalog.tableExists(SOURCE_TABLE):
    raise RuntimeError(
        f"❌ Source table does not exist: {SOURCE_TABLE}"
    )

print("✅ dim_product exists")


# ============================================================
# 3. READ PRODUCT DIMENSION
# ============================================================

product_df = spark.table(SOURCE_TABLE)

print(
    f"dim_product rows: {product_df.count():,}"
)


# ============================================================
# 4. REQUIRED COLUMN VALIDATION
# ============================================================

required_columns = [
    "brand_name"
]

missing_columns = [
    c
    for c in required_columns
    if c not in product_df.columns
]

if missing_columns:
    raise RuntimeError(
        f"❌ Missing columns in dim_product: "
        f"{missing_columns}"
    )

print("✅ Required-column validation passed")


# ============================================================
# 5. NORMALIZE BRAND
# ============================================================
#
# We normalize whitespace and case ONLY for identity.
#
# Example:
#
#   "Dabur"
#   " dabur "
#   "DABUR"
#
# all map to the same brand_key.
#
# We preserve the first observed business-facing name.
# ============================================================

brand_source = (
    product_df
    .select(
        F.trim(
            F.col("brand_name")
            .cast("string")
        ).alias("brand_name")
    )
    .filter(
        F.col("brand_name").isNotNull()
        &
        (F.length(F.col("brand_name")) > 0)
    )
    .withColumn(
        "_normalized_brand",
        F.lower(
            F.regexp_replace(
                F.trim(
                    F.col("brand_name")
                ),
                r"\s+",
                " "
            )
        )
    )
)


# ============================================================
# 6. REMOVE DUPLICATES
# ============================================================

brand_count_before = brand_source.count()

brand_source = (
    brand_source
    .dropDuplicates(
        ["_normalized_brand"]
    )
)

brand_count_after = brand_source.count()


print()
print("BRAND RECONCILIATION")
print("-" * 80)

print(
    f"Brand rows before normalization : "
    f"{brand_count_before:,}"
)

print(
    f"Unique normalized brands        : "
    f"{brand_count_after:,}"
)


# ============================================================
# 7. CREATE DETERMINISTIC BRAND KEY
# ============================================================

brand_df = (
    brand_source
    .withColumn(
        "brand_key",
        F.sha2(
            F.concat(
                F.lit("brand:"),
                F.col("_normalized_brand")
            ),
            256
        )
    )
)


# ============================================================
# 8. SELECT FINAL DISPLAY NAME
# ============================================================
#
# Because _normalized_brand is the identity, the original
# brand_name is retained as the business-facing label.
# ============================================================

brand_df = (
    brand_df
    .select(
        "brand_key",
        "brand_name"
    )
)


# ============================================================
# 9. VALIDATION
# ============================================================

prewrite_count = brand_df.count()

if prewrite_count == 0:
    raise RuntimeError(
        "❌ No brands found in dim_product."
    )


# NULL key validation

null_keys = (
    brand_df
    .filter(
        F.col("brand_key").isNull()
    )
    .count()
)

if null_keys > 0:
    raise RuntimeError(
        f"❌ {null_keys} NULL brand keys found."
    )


# NULL name validation

invalid_names = (
    brand_df
    .filter(
        F.col("brand_name").isNull()
        |
        (
            F.length(
                F.trim(
                    F.col("brand_name")
                )
            ) == 0
        )
    )
    .count()
)

if invalid_names > 0:
    raise RuntimeError(
        f"❌ {invalid_names} invalid brand names found."
    )


# Duplicate key validation

duplicate_keys = (
    brand_df
    .groupBy("brand_key")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

if duplicate_keys > 0:
    raise RuntimeError(
        f"❌ {duplicate_keys} duplicate brand keys found."
    )


print()
print("PRE-WRITE VALIDATION")
print("-" * 80)

print(
    f"Canonical brands: {prewrite_count:,}"
)

print("✅ NULL brand_key validation passed")
print("✅ Brand-name validation passed")
print("✅ Duplicate brand_key validation passed")


# ============================================================
# 10. ADD METADATA
# ============================================================

now_utc = datetime.now(timezone.utc)

brand_df = (
    brand_df
    .withColumn(
        "is_active",
        F.lit(True)
    )
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
    .select(
        "brand_key",
        "brand_name",
        "is_active",
        "created_at",
        "updated_at"
    )
)


# ============================================================
# 11. CREATE / REBUILD TARGET
# ============================================================
#
# This is the first creation of dim_brand.
#
# If the table already exists, rebuild it deterministically
# from dim_product.
#
# This avoids accumulating stale brands that no longer exist
# in the canonical product dimension.
# ============================================================

if spark.catalog.tableExists(TARGET_TABLE):

    print()
    print(
        f"⚠️ Existing {TARGET_TABLE} detected."
    )

    print(
        "Rebuilding from canonical dim_product..."
    )

    spark.sql(
        f"DROP TABLE {TARGET_TABLE}"
    )

    print(
        "✅ Existing dim_brand removed."
    )


# ============================================================
# 12. WRITE
# ============================================================

(
    brand_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

print(
    "✅ dim_brand created successfully"
)


# ============================================================
# 13. POST-WRITE VALIDATION
# ============================================================

final_df = spark.table(TARGET_TABLE)

final_count = final_df.count()

print()
print("=" * 80)
print("POST-WRITE VALIDATION")
print("=" * 80)

print(
    f"Total brands: {final_count:,}"
)


# ============================================================
# 14. FINAL KEY VALIDATION
# ============================================================

final_null_keys = (
    final_df
    .filter(
        F.col("brand_key").isNull()
    )
    .count()
)

if final_null_keys > 0:
    raise RuntimeError(
        f"❌ Final table contains "
        f"{final_null_keys} NULL brand keys."
    )


final_duplicate_keys = (
    final_df
    .groupBy("brand_key")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

if final_duplicate_keys > 0:
    raise RuntimeError(
        f"❌ Final table contains "
        f"{final_duplicate_keys} duplicate brand keys."
    )


# ============================================================
# 15. FINAL NORMALIZED-NAME VALIDATION
# ============================================================

final_normalized_duplicates = (
    final_df
    .withColumn(
        "_normalized_brand",
        F.lower(
            F.regexp_replace(
                F.trim(
                    F.col("brand_name")
                ),
                r"\s+",
                " "
            )
        )
    )
    .groupBy("_normalized_brand")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

if final_normalized_duplicates > 0:
    raise RuntimeError(
        f"❌ Final table contains "
        f"{final_normalized_duplicates} duplicate "
        f"normalized brand names."
    )


print("✅ Final brand_key validation passed")
print("✅ Final normalized-brand validation passed")


# ============================================================
# 16. SAMPLE
# ============================================================

display(
    final_df
    .orderBy("brand_name")
    .limit(200)
)


# ============================================================
# 17. FINAL SUCCESS
# ============================================================

print()
print("=" * 80)
print("18 DIM BRAND SUCCESS")
print("=" * 80)

print(
    f"Canonical brands : {final_count:,}"
)

print(
    f"Source           : {SOURCE_TABLE}"
)

print(
    f"Target           : {TARGET_TABLE}"
)

print("=" * 80)

# COMMAND ----------

