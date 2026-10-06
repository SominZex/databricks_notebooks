# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# ============================================================
# UPDATE PARTNER.CSV FROM LATEST BILLING DATA
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.functions import col


# ============================================================
# CONFIGURATION
# ============================================================

BILLING_TABLE = f"{catalog_name}.{schema_name}.billing_silver"

PARTNER_PATH = (
    "/Volumes/billing/sales_data/new_volume/partner.csv"
)

print("=" * 80)
print("UPDATE PARTNER.CSV FROM LATEST BILLING DATA")
print("=" * 80)

print(f"Billing source : {BILLING_TABLE}")
print(f"Partner file   : {PARTNER_PATH}")
print("=" * 80)


# ============================================================
# 1. VALIDATE BILLING TABLE
# ============================================================

if not spark.catalog.tableExists(BILLING_TABLE):
    raise RuntimeError(
        f"❌ Billing table does not exist: {BILLING_TABLE}"
    )

billing = spark.table(BILLING_TABLE)

required_columns = [
    "orderDate",
    "storeName"
]

missing_columns = [
    c for c in required_columns
    if c not in billing.columns
]

if missing_columns:
    raise RuntimeError(
        f"❌ Missing billing columns: {missing_columns}"
    )


# ============================================================
# 2. FIND LATEST BILLING DATE
# ============================================================

latest_date = (
    billing
    .select(
        F.max("orderDate").alias("max_order_date")
    )
    .collect()[0]["max_order_date"]
)

if latest_date is None:
    raise RuntimeError(
        "❌ No valid orderDate found in billing data."
    )

print(
    f"Latest billing date: {latest_date}"
)


# ============================================================
# 3. NORMALIZE STORE NAME
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
# 4. GET STORES FROM LATEST BILLING DATE
# ============================================================

latest_billing_stores = (
    billing
    .filter(
        col("orderDate") == F.lit(latest_date)
    )
    .select(
        F.trim(
            col("storeName").cast("string")
        ).alias("storeName")
    )
    .filter(
        col("storeName").isNotNull()
        & (col("storeName") != "")
    )
    .withColumn(
        "_normalized_store_name",
        normalize_store_name(col("storeName"))
    )
    .filter(
        col("_normalized_store_name").isNotNull()
        & (col("_normalized_store_name") != "")
    )
    .dropDuplicates(["_normalized_store_name"])
)


latest_store_count = latest_billing_stores.count()

print(
    f"Stores in latest billing date: "
    f"{latest_store_count:,}"
)


# ============================================================
# 5. VALIDATE PARTNER.CSV
# ============================================================

if not dbutils.fs.ls(PARTNER_PATH):
    raise RuntimeError(
        f"❌ Partner file does not exist: {PARTNER_PATH}"
    )


# ============================================================
# 6. READ EXISTING PARTNER.CSV
# ============================================================

partner = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(PARTNER_PATH)
)


required_partner_columns = [
    "storeName",
    "email",
    "wa_number",
    "store"
]

missing_partner_columns = [
    c for c in required_partner_columns
    if c not in partner.columns
]

if missing_partner_columns:
    raise RuntimeError(
        f"❌ partner.csv is missing columns: "
        f"{missing_partner_columns}"
    )


partner = (
    partner
    .select(
        "storeName",
        "email",
        "wa_number",
        "store"
    )
    .withColumn(
        "_normalized_store_name",
        normalize_store_name(col("storeName"))
    )
)


existing_partner_count = partner.count()

print(
    f"Existing partner.csv rows: "
    f"{existing_partner_count:,}"
)


# ============================================================
# 7. FIND STORES MISSING FROM PARTNER.CSV
# ============================================================

missing_stores = (
    latest_billing_stores
    .join(
        partner.select(
            "_normalized_store_name"
        ),
        on="_normalized_store_name",
        how="left_anti"
    )
    .select("storeName")
    .orderBy("storeName")
)


missing_count = missing_stores.count()

print("=" * 80)
print("STORE MASTER COMPARISON")
print("=" * 80)

print(
    f"Latest billing stores : {latest_store_count:,}"
)

print(
    f"Existing partner rows : {existing_partner_count:,}"
)

print(
    f"New stores to add     : {missing_count:,}"
)


# ============================================================
# 8. NOTHING TO ADD
# ============================================================

if missing_count == 0:

    print()
    print(
        "✅ partner.csv is already up to date."
    )

else:

    print()
    print("New stores detected:")

    missing_stores.show(
        100,
        truncate=False
    )


    # ========================================================
    # 9. CREATE NEW PARTNER ROWS
    # ========================================================

    new_partner_rows = (
        missing_stores
        .withColumn(
            "email",
            F.lit("")
        )
        .withColumn(
            "wa_number",
            F.lit("")
        )
        .withColumn(
            "store",
            F.lit("")
        )
        .select(
            "storeName",
            "email",
            "wa_number",
            "store"
        )
    )


    # ========================================================
    # 10. COMBINE EXISTING + NEW
    # ========================================================

    updated_partner = (
        partner
        .drop("_normalized_store_name")
        .unionByName(
            new_partner_rows
        )
    )


    # ========================================================
    # 11. FINAL DUPLICATE VALIDATION
    # ========================================================

    duplicate_partner_stores = (
        updated_partner
        .withColumn(
            "_normalized_store_name",
            normalize_store_name(
                col("storeName")
            )
        )
        .groupBy(
            "_normalized_store_name"
        )
        .count()
        .filter(
            col("count") > 1
        )
    )

    duplicate_count = (
        duplicate_partner_stores.count()
    )

    if duplicate_count > 0:

        raise RuntimeError(
            f"❌ partner.csv update would create "
            f"{duplicate_count:,} duplicate normalized "
            f"store names."
        )


    # ========================================================
    # 12. WRITE UPDATED PARTNER FILE
    # ========================================================

    temp_path = (
        "/Volumes/billing/sales_data/new_volume/"
        "partner_csv_tmp"
    )

    # Remove previous temporary output if present
    try:
        dbutils.fs.rm(
            temp_path,
            True
        )
    except Exception:
        pass


    (
        updated_partner
        .select(
            "storeName",
            "email",
            "wa_number",
            "store"
        )
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(temp_path)
    )


    # ========================================================
    # 13. LOCATE GENERATED CSV
    # ========================================================

    generated_files = dbutils.fs.ls(
        temp_path
    )

    csv_files = [
        f.path
        for f in generated_files
        if f.name.startswith("part-")
        and f.name.endswith(".csv")
    ]

    if len(csv_files) != 1:

        raise RuntimeError(
            "❌ Expected exactly one generated "
            f"CSV file, found {len(csv_files)}."
        )


    generated_csv = csv_files[0]


    # ========================================================
    # 14. BACKUP EXISTING PARTNER.CSV
    # ========================================================

    backup_path = (
        "/Volumes/billing/sales_data/new_volume/"
        "partner_backup.csv"
    )

    try:
        dbutils.fs.rm(
            backup_path,
            True
        )
    except Exception:
        pass


    dbutils.fs.cp(
        PARTNER_PATH,
        backup_path
    )


    # ========================================================
    # 15. REPLACE PARTNER.CSV
    # ========================================================

    dbutils.fs.rm(
        PARTNER_PATH,
        True
    )

    dbutils.fs.cp(
        generated_csv,
        PARTNER_PATH
    )


    # ========================================================
    # 16. CLEAN TEMP
    # ========================================================

    dbutils.fs.rm(
        temp_path,
        True
    )


    # ========================================================
    # 17. POST-WRITE VALIDATION
    # ========================================================

    partner_after = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv(PARTNER_PATH)
    )

    final_count = partner_after.count()

    expected_count = (
        existing_partner_count
        + missing_count
    )

    if final_count != expected_count:

        raise RuntimeError(
            f"❌ partner.csv validation failed. "
            f"Expected {expected_count:,} rows, "
            f"found {final_count:,}."
        )


    print()
    print("=" * 80)
    print("PARTNER.CSV UPDATED SUCCESSFULLY")
    print("=" * 80)

    print(
        f"Previous rows : {existing_partner_count:,}"
    )

    print(
        f"Stores added  : {missing_count:,}"
    )

    print(
        f"Final rows    : {final_count:,}"
    )

    print(
        f"Latest billing date: {latest_date}"
    )

    print(
        f"Backup created: {backup_path}"
    )

    print()
    print(
        "⚠️ Newly discovered stores have blank store IDs."
    )

    print(
        "Update the 'store' column manually before "
        "running 16_dim_store."
    )

    print("=" * 80)

# COMMAND ----------

