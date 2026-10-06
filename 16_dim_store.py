# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# ============================================================
# 16_dim_store
# CANONICAL STORE DIMENSION
#
# Purpose:
#   Create the canonical store identity used by:
#
#       billing
#       stock
#       RTV
#       GRN
#
# Identity:
#
#   store_id + normalized_store_name
#
# IMPORTANT:
#   store_id alone is NOT globally unique.
#
# Example:
#
#   226 + Sonipat GT Road
#   226 + Sonipat TDI City
#
# are different stores.
#
# Source:
#   partner.csv
#
# Target:
#   billing.sales_data.dim_store
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# 1. CONFIG
# ============================================================

CATALOG = "billing"
SCHEMA = "sales_data"

PARTNER_FILE = (
    "/Volumes/billing/sales_data/new_volume/partner.csv"
)

TARGET_TABLE = (
    f"{CATALOG}.{SCHEMA}.dim_store"
)


print("=" * 80)
print("16 DIM STORE")
print("=" * 80)

print(
    f"Partner source : {PARTNER_FILE}"
)

print(
    f"Target         : {TARGET_TABLE}"
)


# ============================================================
# 2. STORE-NAME NORMALIZATION
# ============================================================
#
# This function is deliberately used ONLY for identity
# matching.
#
# Original store_name is preserved separately.
#
# Examples:
#
#   " Vijay Nagar "
#   "VIJAY NAGAR"
#   "Vijay   Nagar"
#   "Vijay Nagar\t"
#
# all normalize to:
#
#   "vijay nagar"
#
# Handles:
#   - leading whitespace
#   - trailing whitespace
#   - repeated whitespace
#   - tabs
#   - newlines
#   - carriage returns
#   - non-breaking spaces
#   - case differences
# ============================================================

def normalize_store_name(column):

    value = column.cast("string")

    # --------------------------------------------------------
    # 1. Remove invisible Unicode characters
    # --------------------------------------------------------
    value = F.regexp_replace(
        value,
        r"[\u200B\u200C\u200D\uFEFF]",
        ""
    )

    # --------------------------------------------------------
    # 2. Convert all common Unicode whitespace/control
    #    characters to a normal space
    # --------------------------------------------------------
    value = F.regexp_replace(
        value,
        r"[\t\r\n\f\v\u00A0\u1680\u2000-\u200A\u2028\u2029\u202F\u205F\u3000]+",
        " "
    )

    # --------------------------------------------------------
    # 3. Collapse repeated whitespace
    # --------------------------------------------------------
    value = F.regexp_replace(
        value,
        r"\s+",
        " "
    )

    # --------------------------------------------------------
    # 4. Remove leading/trailing whitespace
    # --------------------------------------------------------
    value = F.trim(value)

    # --------------------------------------------------------
    # 5. Normalize case
    # --------------------------------------------------------
    value = F.lower(value)

    return value

# ============================================================
# 3. READ PARTNER MASTER
# ============================================================

partner = (
    spark.read
    .option(
        "header",
        True
    )
    .option(
        "inferSchema",
        True
    )
    .csv(
        PARTNER_FILE
    )
)


print()
print(
    f"Partner rows loaded: "
    f"{partner.count():,}"
)


# ============================================================
# 4. VERIFY REQUIRED COLUMNS
# ============================================================

required_columns = [
    "storeName",
    "email",
    "wa_number",
    "store"
]

missing_columns = [
    c
    for c in required_columns
    if c not in partner.columns
]

if missing_columns:

    raise RuntimeError(
        "❌ partner.csv is missing required columns: "
        + ", ".join(missing_columns)
    )


# ============================================================
# 5. STANDARDIZE PARTNER DATA
# ============================================================

stores = (
    partner

    # --------------------------------------------------------
    # Preserve original store name
    # --------------------------------------------------------

    .withColumn(
        "store_name",
        normalize_store_name(F.col("storeName"))
    )

    # --------------------------------------------------------
    # Store ID
    # --------------------------------------------------------

    .withColumn(
        "store_id",
        F.col("store")
        .cast("string")
    )

    # --------------------------------------------------------
    # Canonical normalized store name
    # --------------------------------------------------------

    .withColumn(
        "normalized_store_name",
        normalize_store_name(
            F.col("storeName")
        )
    )

    # --------------------------------------------------------
    # Contact fields
    # --------------------------------------------------------

    .withColumn(
        "email",
        F.trim(
            F.col("email")
            .cast("string")
        )
    )

    .withColumn(
        "wa_number",
        F.col("wa_number")
        .cast("string")
    )
)


# ============================================================
# 6. SOURCE VALIDATION
# ============================================================
#
# IMPORTANT:
#
# partner.csv may temporarily contain stores discovered from
# latest billing data whose store IDs have not yet been manually
# assigned.
#
# These rows must NOT break the pipeline.
#
# Rules:
#   - Missing store ID  → SKIP
#   - Empty store ID    → SKIP
#   - Missing store name → FAIL
#   - Empty store name   → FAIL
#
# Only stores with a valid store ID become part of dim_store.
# ============================================================


# ------------------------------------------------------------
# Identify rows with missing store IDs
# ------------------------------------------------------------

missing_store_id_rows = (
    stores
    .filter(
        F.col("store_id").isNull()
        |
        (F.trim(F.col("store_id")) == "")
    )
)

missing_store_id_count = (
    missing_store_id_rows.count()
)


# ------------------------------------------------------------
# Identify invalid store names
# ------------------------------------------------------------

invalid_store_name_rows = (
    stores
    .filter(
        F.col("store_name").isNull()
        |
        (F.trim(F.col("store_name")) == "")
        |
        F.col("normalized_store_name").isNull()
        |
        (F.trim(F.col("normalized_store_name")) == "")
    )
)

invalid_store_name_count = (
    invalid_store_name_rows.count()
)


print()
print("=" * 80)
print("SOURCE VALIDATION")
print("=" * 80)

print(
    f"Partner rows loaded       : "
    f"{stores.count():,}"
)

print(
    f"Missing store ID rows     : "
    f"{missing_store_id_count:,}"
)

print(
    f"Invalid store name rows   : "
    f"{invalid_store_name_count:,}"
)


# ------------------------------------------------------------
# Invalid store name is still a hard failure
# ------------------------------------------------------------

if invalid_store_name_count > 0:

    print()
    print("Invalid store-name records:")

    display(
        invalid_store_name_rows
        .select(
            "store_name",
            "store_id",
            "email",
            "wa_number"
        )
    )

    raise RuntimeError(
        f"❌ {invalid_store_name_count:,} rows contain "
        "invalid store names."
    )


# ------------------------------------------------------------
# Missing store IDs are intentionally skipped
# ------------------------------------------------------------

if missing_store_id_count > 0:

    print()
    print(
        "⚠️ Stores without store IDs will be skipped "
        "from dim_store:"
    )

    display(
        missing_store_id_rows
        .select(
            "store_name",
            "store_id",
            "email",
            "wa_number"
        )
        .orderBy("store_name")
    )


# ------------------------------------------------------------
# Keep ONLY stores with valid IDs
# ------------------------------------------------------------

stores = (
    stores
    .filter(
        F.col("store_id").isNotNull()
        &
        (F.trim(F.col("store_id")) != "")
    )
)


valid_store_count = stores.count()

print()
print(
    f"Valid stores entering dim_store: "
    f"{valid_store_count:,}"
)

print(
    f"Skipped stores without IDs     : "
    f"{missing_store_id_count:,}"
)

print("=" * 80)

# ============================================================
# 7. CANONICAL STORE IDENTITY
# ============================================================
#
# The store key MUST include:
#
#   store_id + normalized_store_name
#
# because raw store IDs are not globally unique.
#
# Example:
#
#   226|sonipat gt road
#   226|sonipat tdi city
#
# produce different store keys.
# ============================================================

stores = (
    stores

    .withColumn(
        "_identity_string",
        F.concat_ws(
            "|",
            F.col("store_id"),
            F.col("normalized_store_name")
        )
    )

    .withColumn(
        "store_key",
        F.sha2(
            F.concat(
                F.lit("store:"),
                F.col("_identity_string")
            ),
            256
        )
    )
)


# ============================================================
# 8. DUPLICATE CANONICAL IDENTITY CHECK
# ============================================================

duplicate_identity = (
    stores
    .groupBy(
        "store_id",
        "normalized_store_name"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
)


duplicate_identity_count = (
    duplicate_identity.count()
)


print(
    f"Duplicate canonical identities: "
    f"{duplicate_identity_count:,}"
)


if duplicate_identity_count > 0:

    display(
        duplicate_identity
        .orderBy(
            F.desc("count")
        )
    )

    raise RuntimeError(
        "❌ Duplicate store_id + normalized_store_name "
        "identities found in partner.csv."
    )


# ============================================================
# 9. DUPLICATE STORE KEY CHECK
# ============================================================

duplicate_keys = (
    stores
    .groupBy(
        "store_key"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


if duplicate_keys > 0:

    raise RuntimeError(
        f"❌ {duplicate_keys:,} duplicate store_key values."
    )


# ============================================================
# 10. NORMALIZED NAME COLLISION CHECK
# ============================================================
#
# This is important.
#
# Two different store IDs could theoretically normalize to
# the same store name.
#
# Example:
#
#   101 + "Vijay Nagar"
#   202 + "VIJAY   NAGAR"
#
# These remain two separate stores because store_id is part
# of the identity.
#
# We report these cases instead of merging them.
# ============================================================

normalized_name_collisions = (
    stores
    .groupBy(
        "normalized_store_name"
    )
    .agg(
        F.countDistinct(
            "store_id"
        ).alias(
            "distinct_store_ids"
        ),
        F.collect_set(
            "store_id"
        ).alias(
            "store_ids"
        ),
        F.collect_set(
            "store_name"
        ).alias(
            "store_names"
        )
    )
    .filter(
        F.col("distinct_store_ids") > 1
    )
)


collision_count = (
    normalized_name_collisions.count()
)


print(
    f"Normalized-name collisions across store IDs: "
    f"{collision_count:,}"
)


if collision_count > 0:

    print()
    print(
        "These are NOT automatically merged:"
    )

    display(
        normalized_name_collisions
    )


# ============================================================
# 11. BUILD FINAL DIMENSION
# ============================================================

dim_store = (
    stores

    .select(

        # ----------------------------------------------------
        # Canonical identity
        # ----------------------------------------------------

        "store_key",
        "store_id",
        "store_name",
        "normalized_store_name",

        # ----------------------------------------------------
        # Partner information
        # ----------------------------------------------------

        "email",
        "wa_number",

        # ----------------------------------------------------
        # Current partner master = active
        # ----------------------------------------------------

        F.lit(True)
        .cast("boolean")
        .alias("is_active"),

        # ----------------------------------------------------
        # Audit timestamps
        # ----------------------------------------------------

        F.current_timestamp()
        .alias("created_at"),

        F.current_timestamp()
        .alias("updated_at")
    )
)


# ============================================================
# 12. FINAL NULL VALIDATION
# ============================================================

null_identity_rows = (
    dim_store
    .filter(
        F.col("store_key").isNull()
        |
        F.col("store_id").isNull()
        |
        F.col("store_name").isNull()
        |
        F.col("normalized_store_name").isNull()
    )
    .count()
)


if null_identity_rows > 0:

    raise RuntimeError(
        f"❌ {null_identity_rows:,} rows contain "
        "NULL canonical store identity values."
    )


# ============================================================
# 13. FINAL ROW COUNT
# ============================================================

final_source_count = (
    stores.count()
)

final_dimension_count = (
    dim_store.count()
)


print()
print(
    f"Source rows : "
    f"{final_source_count:,}"
)

print(
    f"Dimension rows: "
    f"{final_dimension_count:,}"
)


if final_source_count != final_dimension_count:

    raise RuntimeError(
        "❌ Store dimension row count changed unexpectedly."
    )


# ============================================================
# 14. DROP OLD DIMENSION
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
        "Existing dim_store dropped."
    )


# ============================================================
# 15. WRITE DELTA TABLE
# ============================================================

(
    dim_store
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        TARGET_TABLE
    )
)


print()
print(
    "✅ dim_store written successfully."
)


# ============================================================
# 16. POST-WRITE VALIDATION
# ============================================================

final_dim = (
    spark.table(
        TARGET_TABLE
    )
)


final_count = (
    final_dim.count()
)


final_duplicate_keys = (
    final_dim
    .groupBy(
        "store_key"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


final_duplicate_identities = (
    final_dim
    .groupBy(
        "store_id",
        "normalized_store_name"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)


final_null_keys = (
    final_dim
    .filter(
        F.col("store_key").isNull()
        |
        F.col("store_id").isNull()
        |
        F.col("normalized_store_name").isNull()
    )
    .count()
)


# ============================================================
# 17. FINAL ASSERTIONS
# ============================================================

if final_count != final_source_count:

    raise RuntimeError(
        f"❌ Final row count mismatch. "
        f"Expected {final_source_count:,}, "
        f"got {final_count:,}."
    )


if final_duplicate_keys > 0:

    raise RuntimeError(
        f"❌ Final dimension contains "
        f"{final_duplicate_keys:,} duplicate store keys."
    )


if final_duplicate_identities > 0:

    raise RuntimeError(
        f"❌ Final dimension contains "
        f"{final_duplicate_identities:,} duplicate "
        "canonical identities."
    )


if final_null_keys > 0:

    raise RuntimeError(
        f"❌ Final dimension contains "
        f"{final_null_keys:,} NULL identity rows."
    )


# ============================================================
# 18. DISPLAY SAMPLE
# ============================================================

print()
print("=" * 80)
print("FINAL DIM_STORE SAMPLE")
print("=" * 80)

display(
    final_dim
    .orderBy(
        "store_name"
    )
    .limit(30)
)


# ============================================================
# 19. SUCCESS
# ============================================================

print()
print("=" * 80)
print("16 DIM STORE SUCCESS")
print("=" * 80)

print(
    f"Store rows             : {final_count:,}"
)

print(
    f"Unique store keys      : "
    f"{final_dim.select('store_key').distinct().count():,}"
)

print(
    f"Unique normalized names: "
    f"{final_dim.select('normalized_store_name').distinct().count():,}"
)

print(
    f"Name collisions        : "
    f"{collision_count:,}"
)

print(
    "Identity               : "
    "store_id + normalized_store_name"
)

print(
    "Normalization          : "
    "trim + whitespace collapse + lowercase"
)

print(
    f"Target                 : "
    f"{TARGET_TABLE}"
)

print("=" * 80)

# COMMAND ----------

