# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# ============================================================
# 20_dim_vendor
# CANONICAL VENDOR DIMENSION
#
# Sources:
#   stock_silver.vendor_name
#   rtv_silver.vendorName
#   grn_data.vendor
#
# Target:
#   billing.sales_data.dim_vendor
#
# Grain:
#   1 row per normalized vendor
#
# Identity:
#   normalized vendor name
# ============================================================

from pyspark.sql import functions as F
from datetime import datetime, timezone


# ============================================================
# 1. CONFIG
# ============================================================

CATALOG = "billing"
SCHEMA = "sales_data"

STOCK_TABLE = f"{CATALOG}.{SCHEMA}.stock_silver"
RTV_TABLE = f"{CATALOG}.{SCHEMA}.rtv_silver"
GRN_TABLE = f"{CATALOG}.{SCHEMA}.grn_data"

TARGET_TABLE = f"{CATALOG}.{SCHEMA}.dim_vendor"


print("=" * 80)
print("20 DIM VENDOR")
print("=" * 80)

print(f"Stock : {STOCK_TABLE}")
print(f"RTV   : {RTV_TABLE}")
print(f"GRN   : {GRN_TABLE}")
print(f"Target: {TARGET_TABLE}")


# ============================================================
# 2. CHECK SOURCE TABLES
# ============================================================

sources = {
    "stock": STOCK_TABLE,
    "rtv": RTV_TABLE,
    "grn": GRN_TABLE,
}

for source_name, table_name in sources.items():

    if not spark.catalog.tableExists(table_name):
        raise RuntimeError(
            f"❌ Required source table does not exist: {table_name}"
        )

    print(
        f"✅ {source_name}: {table_name}"
    )


# ============================================================
# 3. READ SOURCES
# ============================================================

stock = spark.table(STOCK_TABLE)
rtv = spark.table(RTV_TABLE)
grn = spark.table(GRN_TABLE)


print()
print("SOURCE ROW COUNTS")
print("-" * 80)

print(f"Stock : {stock.count():,}")
print(f"RTV   : {rtv.count():,}")
print(f"GRN   : {grn.count():,}")


# ============================================================
# 4. REQUIRED COLUMN VALIDATION
# ============================================================

required_columns = {
    "stock": ["vendor_name"],
    "rtv": ["vendorName"],
    "grn": ["vendor"],
}

for source_name, columns in required_columns.items():

    table_name = sources[source_name]
    df = spark.table(table_name)

    missing = [
        c for c in columns
        if c not in df.columns
    ]

    if missing:
        raise RuntimeError(
            f"❌ {source_name} missing columns: {missing}"
        )


print("✅ Source schema validation passed")


# ============================================================
# 5. CLEAN VENDOR HELPER
# ============================================================

def clean_vendor(column_name):

    c = F.trim(
        F.col(column_name)
        .cast("string")
    )

    return (
        F.when(
            c.isNull()
            |
            (c == "")
            |
            F.lower(c).isin(
                "null",
                "none",
                "nan",
                "n/a",
                "na"
            ),
            F.lit(None)
        )
        .otherwise(c)
    )


# ============================================================
# 6. STOCK VENDORS
# ============================================================

stock_vendors = (
    stock
    .select(
        clean_vendor("vendor_name")
        .alias("vendor_name")
    )
    .filter(
        F.col("vendor_name").isNotNull()
    )
    .withColumn(
        "source_system",
        F.lit("stock")
    )
)


# ============================================================
# 7. RTV VENDORS
# ============================================================

rtv_vendors = (
    rtv
    .select(
        clean_vendor("vendorName")
        .alias("vendor_name")
    )
    .filter(
        F.col("vendor_name").isNotNull()
    )
    .withColumn(
        "source_system",
        F.lit("rtv")
    )
)


# ============================================================
# 8. GRN VENDORS
# ============================================================

grn_vendors = (
    grn
    .select(
        clean_vendor("vendor")
        .alias("vendor_name")
    )
    .filter(
        F.col("vendor_name").isNotNull()
    )
    .withColumn(
        "source_system",
        F.lit("grn")
    )
)


# ============================================================
# 9. UNION VENDOR SOURCES
# ============================================================

all_vendors = (
    stock_vendors
    .unionByName(rtv_vendors)
    .unionByName(grn_vendors)
)


print()
print(
    f"Combined vendor-source rows: "
    f"{all_vendors.count():,}"
)


# ============================================================
# 10. NORMALIZE VENDOR IDENTITY
# ============================================================
#
# Case and repeated whitespace are ignored for identity.
#
# Example:
#
#   "ABC Foods"
#   " abc foods "
#   "ABC   FOODS"
#
# → same vendor_key
#
# We do NOT automatically merge semantic variations such as:
#
#   "ABC Foods Pvt Ltd"
#   "ABC Foods"
#
# because that requires business/master-data confirmation.
# ============================================================

all_vendors = (
    all_vendors
    .withColumn(
        "_normalized_vendor",
        F.lower(
            F.regexp_replace(
                F.trim(
                    F.col("vendor_name")
                ),
                r"\s+",
                " "
            )
        )
    )
)


# ============================================================
# 11. RECONCILE VENDOR NAMES
# ============================================================

vendor_reconciliation = (
    all_vendors
    .groupBy("_normalized_vendor")
    .agg(
        F.countDistinct(
            "vendor_name"
        ).alias("name_variants"),

        F.collect_set(
            "source_system"
        ).alias("source_systems")
    )
)


variant_count = (
    vendor_reconciliation
    .filter(
        F.col("name_variants") > 1
    )
    .count()
)


print()
print("VENDOR RECONCILIATION")
print("-" * 80)

print(
    f"Normalized vendors with multiple "
    f"name variants: {variant_count:,}"
)


# ============================================================
# 12. BUILD CANONICAL VENDOR
# ============================================================

vendor_df = (
    all_vendors
    .groupBy("_normalized_vendor")
    .agg(
        F.first(
            "vendor_name",
            ignorenulls=True
        ).alias("vendor_name"),

        F.collect_set(
            "source_system"
        ).alias("_source_systems")
    )
)


# ============================================================
# 13. CREATE DETERMINISTIC VENDOR KEY
# ============================================================

vendor_df = (
    vendor_df
    .withColumn(
        "vendor_key",
        F.sha2(
            F.concat(
                F.lit("vendor:"),
                F.col("_normalized_vendor")
            ),
            256
        )
    )
)


# ============================================================
# 14. FINAL COLUMN SET
# ============================================================

vendor_df = (
    vendor_df
    .select(
        "vendor_key",
        "vendor_name"
    )
)


# ============================================================
# 15. PRE-WRITE VALIDATION
# ============================================================

prewrite_count = vendor_df.count()

if prewrite_count == 0:
    raise RuntimeError(
        "❌ No vendors found."
    )


null_keys = (
    vendor_df
    .filter(
        F.col("vendor_key").isNull()
    )
    .count()
)

if null_keys > 0:
    raise RuntimeError(
        f"❌ {null_keys} NULL vendor keys found."
    )


invalid_names = (
    vendor_df
    .filter(
        F.col("vendor_name").isNull()
        |
        (
            F.length(
                F.trim(
                    F.col("vendor_name")
                )
            ) == 0
        )
    )
    .count()
)

if invalid_names > 0:
    raise RuntimeError(
        f"❌ {invalid_names} invalid vendor names found."
    )


duplicate_keys = (
    vendor_df
    .groupBy("vendor_key")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

if duplicate_keys > 0:
    raise RuntimeError(
        f"❌ {duplicate_keys} duplicate vendor keys found."
    )


print()
print("PRE-WRITE VALIDATION")
print("-" * 80)

print(
    f"Canonical vendors: {prewrite_count:,}"
)

print("✅ vendor_key NULL validation passed")
print("✅ vendor_name validation passed")
print("✅ vendor_key uniqueness validation passed")


# ============================================================
# 16. ADD METADATA
# ============================================================

now_utc = datetime.now(timezone.utc)

vendor_df = (
    vendor_df
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
        "vendor_key",
        "vendor_name",
        "is_active",
        "created_at",
        "updated_at"
    )
)


# ============================================================
# 17. CREATE / REBUILD TARGET
# ============================================================

if spark.catalog.tableExists(TARGET_TABLE):

    print()
    print(
        f"⚠️ Existing {TARGET_TABLE} detected."
    )

    print(
        "Rebuilding from current source systems..."
    )

    spark.sql(
        f"DROP TABLE {TARGET_TABLE}"
    )

    print(
        "✅ Existing dim_vendor removed."
    )


# ============================================================
# 18. WRITE DELTA TABLE
# ============================================================

(
    vendor_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

print(
    "✅ dim_vendor created successfully"
)


# ============================================================
# 19. POST-WRITE VALIDATION
# ============================================================

final_df = spark.table(TARGET_TABLE)

final_count = final_df.count()

print()
print("=" * 80)
print("POST-WRITE VALIDATION")
print("=" * 80)

print(
    f"Total vendors: {final_count:,}"
)


# ============================================================
# 20. FINAL KEY VALIDATION
# ============================================================

final_null_keys = (
    final_df
    .filter(
        F.col("vendor_key").isNull()
    )
    .count()
)

if final_null_keys > 0:
    raise RuntimeError(
        f"❌ Final table contains "
        f"{final_null_keys} NULL vendor keys."
    )


final_duplicate_keys = (
    final_df
    .groupBy("vendor_key")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

if final_duplicate_keys > 0:
    raise RuntimeError(
        f"❌ Final table contains "
        f"{final_duplicate_keys} duplicate vendor keys."
    )


# ============================================================
# 21. FINAL NORMALIZED NAME VALIDATION
# ============================================================

final_normalized_duplicates = (
    final_df
    .withColumn(
        "_normalized_vendor",
        F.lower(
            F.regexp_replace(
                F.trim(
                    F.col("vendor_name")
                ),
                r"\s+",
                " "
            )
        )
    )
    .groupBy("_normalized_vendor")
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
        f"normalized vendor names."
    )


print("✅ Final vendor_key validation passed")
print("✅ Final normalized-vendor validation passed")


# ============================================================
# 22. SAMPLE
# ============================================================

display(
    final_df
    .orderBy("vendor_name")
    .limit(200)
)


# ============================================================
# 23. FINAL SUCCESS
# ============================================================

print()
print("=" * 80)
print("20 DIM VENDOR SUCCESS")
print("=" * 80)

print(
    f"Canonical vendors : {final_count:,}"
)

print(
    f"Source systems    : Stock + RTV + GRN"
)

print(
    f"Target            : {TARGET_TABLE}"
)

print("=" * 80)

# COMMAND ----------

