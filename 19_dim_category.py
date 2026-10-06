# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# ============================================================
# 19_dim_category
# CANONICAL CATEGORY DIMENSION
#
# Source:
#   billing.sales_data.dim_product
#
# Target:
#   billing.sales_data.dim_category
#
# Grain:
#   1 row per category + sub-category
#
# Identity:
#   normalized category_name + normalized sub_category
# ============================================================

from pyspark.sql import functions as F
from datetime import datetime, timezone


# ============================================================
# 1. CONFIG
# ============================================================

CATALOG = "billing"
SCHEMA = "sales_data"

SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.dim_product"
TARGET_TABLE = f"{CATALOG}.{SCHEMA}.dim_category"


print("=" * 80)
print("19 DIM CATEGORY")
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
# 4. REQUIRED COLUMNS
# ============================================================

required_columns = [
    "category_name",
    "sub_category"
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
# 5. CLEAN CATEGORY VALUES
# ============================================================

category_source = (
    product_df
    .select(
        F.trim(
            F.col("category_name")
            .cast("string")
        ).alias("category_name"),

        F.trim(
            F.col("sub_category")
            .cast("string")
        ).alias("sub_category")
    )
)


# ============================================================
# 6. NORMALIZE BLANK VALUES
# ============================================================

category_source = (
    category_source

    .withColumn(
        "category_name",
        F.when(
            F.col("category_name").isNull()
            |
            (F.length(F.col("category_name")) == 0),
            F.lit("UNKNOWN")
        )
        .otherwise(
            F.col("category_name")
        )
    )

    .withColumn(
        "sub_category",
        F.when(
            F.col("sub_category").isNull()
            |
            (F.length(F.col("sub_category")) == 0),
            F.lit("UNKNOWN")
        )
        .otherwise(
            F.col("sub_category")
        )
    )
)


# ============================================================
# 7. CREATE NORMALIZED IDENTITY
# ============================================================
#
# Identity:
#
#     category + sub_category
#
# Case and repeated whitespace are ignored for the key.
#
# Display values remain human-readable.
# ============================================================

category_source = (
    category_source

    .withColumn(
        "_category_norm",
        F.lower(
            F.regexp_replace(
                F.trim(
                    F.col("category_name")
                ),
                r"\s+",
                " "
            )
        )
    )

    .withColumn(
        "_subcategory_norm",
        F.lower(
            F.regexp_replace(
                F.trim(
                    F.col("sub_category")
                ),
                r"\s+",
                " "
            )
        )
    )
)


# ============================================================
# 8. CREATE CATEGORY KEY
# ============================================================

category_df = (
    category_source

    .withColumn(
        "category_key",
        F.sha2(
            F.concat(
                F.lit("category:"),
                F.col("_category_norm"),
                F.lit("|"),
                F.col("_subcategory_norm")
            ),
            256
        )
    )

    .dropDuplicates(
        ["category_key"]
    )
)


# ============================================================
# 9. VALIDATE CATEGORY COUNT
# ============================================================

prewrite_count = category_df.count()

if prewrite_count == 0:
    raise RuntimeError(
        "❌ No categories found in dim_product."
    )

print()
print("CATEGORY RECONCILIATION")
print("-" * 80)

print(
    f"Canonical category combinations: "
    f"{prewrite_count:,}"
)


# ============================================================
# 10. FINAL COLUMN SET
# ============================================================

category_df = (
    category_df
    .select(
        "category_key",
        "category_name",
        "sub_category"
    )
)


# ============================================================
# 11. PRE-WRITE VALIDATION
# ============================================================

null_keys = (
    category_df
    .filter(
        F.col("category_key").isNull()
    )
    .count()
)

if null_keys > 0:
    raise RuntimeError(
        f"❌ {null_keys} NULL category keys found."
    )


duplicate_keys = (
    category_df
    .groupBy("category_key")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

if duplicate_keys > 0:
    raise RuntimeError(
        f"❌ {duplicate_keys} duplicate category keys found."
    )


invalid_category_names = (
    category_df
    .filter(
        F.col("category_name").isNull()
        |
        (F.length(F.trim(F.col("category_name"))) == 0)
    )
    .count()
)

if invalid_category_names > 0:
    raise RuntimeError(
        f"❌ {invalid_category_names} invalid category names."
    )


invalid_subcategories = (
    category_df
    .filter(
        F.col("sub_category").isNull()
        |
        (F.length(F.trim(F.col("sub_category"))) == 0)
    )
    .count()
)

if invalid_subcategories > 0:
    raise RuntimeError(
        f"❌ {invalid_subcategories} invalid sub-category names."
    )


print()
print("PRE-WRITE VALIDATION")
print("-" * 80)

print(
    f"Canonical category combinations: "
    f"{prewrite_count:,}"
)

print("✅ category_key NULL validation passed")
print("✅ category_key uniqueness validation passed")
print("✅ category_name validation passed")
print("✅ sub_category validation passed")


# ============================================================
# 12. ADD METADATA
# ============================================================

now_utc = datetime.now(timezone.utc)

category_df = (
    category_df
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
        "category_key",
        "category_name",
        "sub_category",
        "is_active",
        "created_at",
        "updated_at"
    )
)


# ============================================================
# 13. CREATE / REBUILD TARGET
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
        "✅ Existing dim_category removed."
    )


# ============================================================
# 14. WRITE DELTA TABLE
# ============================================================

(
    category_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

print(
    "✅ dim_category created successfully"
)


# ============================================================
# 15. POST-WRITE VALIDATION
# ============================================================

final_df = spark.table(TARGET_TABLE)

final_count = final_df.count()

print()
print("=" * 80)
print("POST-WRITE VALIDATION")
print("=" * 80)

print(
    f"Total category combinations: "
    f"{final_count:,}"
)


# ============================================================
# 16. FINAL KEY VALIDATION
# ============================================================

final_null_keys = (
    final_df
    .filter(
        F.col("category_key").isNull()
    )
    .count()
)

if final_null_keys > 0:
    raise RuntimeError(
        f"❌ Final table contains "
        f"{final_null_keys} NULL category keys."
    )


final_duplicate_keys = (
    final_df
    .groupBy("category_key")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

if final_duplicate_keys > 0:
    raise RuntimeError(
        f"❌ Final table contains "
        f"{final_duplicate_keys} duplicate category keys."
    )


# ============================================================
# 17. NORMALIZED DUPLICATE VALIDATION
# ============================================================

normalized_duplicates = (
    final_df

    .withColumn(
        "_category_norm",
        F.lower(
            F.regexp_replace(
                F.trim(
                    F.col("category_name")
                ),
                r"\s+",
                " "
            )
        )
    )

    .withColumn(
        "_subcategory_norm",
        F.lower(
            F.regexp_replace(
                F.trim(
                    F.col("sub_category")
                ),
                r"\s+",
                " "
            )
        )
    )

    .groupBy(
        "_category_norm",
        "_subcategory_norm"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


if normalized_duplicates > 0:
    raise RuntimeError(
        f"❌ {normalized_duplicates} duplicate "
        f"normalized category combinations found."
    )


print("✅ Final category_key validation passed")
print("✅ Final normalized-category validation passed")


# ============================================================
# 18. CATEGORY COVERAGE
# ============================================================

distinct_categories = (
    final_df
    .select("category_name")
    .distinct()
    .count()
)

distinct_subcategories = (
    final_df
    .select("sub_category")
    .distinct()
    .count()
)

print()
print("CATEGORY COVERAGE")
print("-" * 80)

print(
    f"Distinct categories     : "
    f"{distinct_categories:,}"
)

print(
    f"Distinct sub-categories : "
    f"{distinct_subcategories:,}"
)


# ============================================================
# 19. SAMPLE
# ============================================================

display(
    final_df
    .orderBy(
        "category_name",
        "sub_category"
    )
    .limit(200)
)


# ============================================================
# 20. FINAL SUCCESS
# ============================================================

print()
print("=" * 80)
print("19 DIM CATEGORY SUCCESS")
print("=" * 80)

print(
    f"Category combinations : {final_count:,}"
)

print(
    f"Distinct categories   : {distinct_categories:,}"
)

print(
    f"Distinct subcategories: {distinct_subcategories:,}"
)

print(
    f"Source                : {SOURCE_TABLE}"
)

print(
    f"Target                : {TARGET_TABLE}"
)

print("=" * 80)

# COMMAND ----------

