# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# ============================================================
# 24_fact_grn
# GRN → Fact GRN
#
# Grain:
#   product × store × grn_date
#
# Source:
#   billing.sales_data.grn_data
#
# Dimensions:
#   billing.sales_data.dim_product
#   billing.sales_data.dim_store
#   billing.sales_data.dim_date
#
# Target:
#   billing.sales_data.fact_grn
#
# Key properties:
#   - Idempotent
#   - Store-name normalization
#   - Product barcode normalization
#   - Date dimension validation
#   - Unmatched GRN products are diagnosed and excluded
#   - Duplicate source grain consolidation
#   - Partition-safe replacement by grn_date
#   - Strong post-write validation
# ============================================================


from pyspark.sql import functions as F
from delta.tables import DeltaTable


# ============================================================
# 1. CONFIGURATION
# ============================================================

SOURCE_TABLE = "billing.sales_data.grn_data"

PRODUCT_DIM = "billing.sales_data.dim_product"
STORE_DIM = "billing.sales_data.dim_store"
DATE_DIM = "billing.sales_data.dim_date"

TARGET_TABLE = "billing.sales_data.fact_grn"


# ============================================================
# 2. HELPERS
# ============================================================

def normalize_store_name(column):
    """
    Canonical store-name normalization.

    Handles:
      - NULL
      - tabs
      - CR/LF
      - NBSP
      - repeated whitespace
      - leading/trailing whitespace
      - case differences
    """

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


def normalize_barcode(column):
    """
    Canonical barcode normalization.

    Handles:
      - NULL
      - whitespace
      - numeric values represented as strings
      - trailing '.0'
    """

    return (
        F.when(
            column.isNull(),
            F.lit(None).cast("string")
        )
        .otherwise(
            F.regexp_replace(
                F.trim(column.cast("string")),
                r"\.0+$",
                ""
            )
        )
    )


def assert_table_exists(table_name):

    if not spark.catalog.tableExists(table_name):

        raise RuntimeError(
            f"❌ Required table does not exist: {table_name}"
        )


def assert_no_duplicate_grain(df, grain_columns, label):

    duplicate_rows = (
        df
        .groupBy(*grain_columns)
        .count()
        .filter(F.col("count") > 1)
    )

    duplicate_count = duplicate_rows.count()

    if duplicate_count > 0:

        print(
            f"\n❌ {label}: "
            f"{duplicate_count:,} duplicate grain combinations"
        )

        duplicate_rows.show(
            20,
            truncate=False
        )

        raise RuntimeError(
            f"❌ {label} contains duplicate grain combinations: "
            f"{duplicate_count:,}"
        )

    print(
        f"✅ {label}: duplicate grain check passed"
    )


# ============================================================
# 3. REQUIRED TABLE VALIDATION
# ============================================================

print("\n" + "=" * 80)
print("24_FACT_GRN")
print("=" * 80)

print("\n[1/18] Validating required tables...")

for table in [
    SOURCE_TABLE,
    PRODUCT_DIM,
    STORE_DIM,
    DATE_DIM
]:

    assert_table_exists(table)

print("✅ All required tables exist")


# ============================================================
# 4. LOAD SOURCE
# ============================================================

print("\n[2/18] Loading GRN source...")

grn_raw = spark.table(SOURCE_TABLE)

raw_grn_count = grn_raw.count()

print(
    f"Raw GRN rows: "
    f"{raw_grn_count:,}"
)

if raw_grn_count == 0:

    raise RuntimeError(
        "❌ GRN source table contains zero rows."
    )


# ============================================================
# 5. VALIDATE SOURCE COLUMNS
# ============================================================

print("\n[3/18] Validating GRN schema...")

required_grn_columns = [

    "Date",
    "Time",
    "barcode",
    "productName",
    "recievedQuantity",
    "sellingPrice",
    "costPrice",
    "totalCost",
    "gst",
    "gstAmount",
    "cess",
    "cessAmount",
    "acess",
    "acessAmount",
    "totalTaxes",
    "amountNet",
    "grnId",
    "invoiceNumber",
    "storeName",
    "brandName",
    "categoryName",
    "vendor",
    "vendorGst",
    "user"

]

missing_columns = [

    c
    for c in required_grn_columns
    if c not in grn_raw.columns

]

if missing_columns:

    raise RuntimeError(
        f"❌ Missing required GRN columns: "
        f"{missing_columns}"
    )

print("✅ GRN schema validated")


# ============================================================
# 6. PREPARE / NORMALIZE SOURCE
# ============================================================

print("\n[4/18] Normalizing GRN source...")

grn = (

    grn_raw

    # --------------------------------------------------------
    # Date
    # --------------------------------------------------------

    .withColumn(
        "grn_date",

        F.coalesce(

            F.col("Date").cast("date"),

            F.to_date(
                F.trim(
                    F.col("Date").cast("string")
                ),
                "M/d/yyyy"
            ),

            F.to_date(
                F.trim(
                    F.col("Date").cast("string")
                ),
                "MM/dd/yyyy"
            ),

            F.to_date(
                F.trim(
                    F.col("Date").cast("string")
                ),
                "yyyy-MM-dd"
            )

        )
    )

    # --------------------------------------------------------
    # Barcode
    # --------------------------------------------------------

    .withColumn(
        "_barcode",
        normalize_barcode(
            F.col("barcode")
        )
    )

    # --------------------------------------------------------
    # Store
    # --------------------------------------------------------

    .withColumn(
        "_store_name_normalized",
        normalize_store_name(
            F.col("storeName")
        )
    )

    # --------------------------------------------------------
    # Numeric fields
    # --------------------------------------------------------

    .withColumn(
        "_received_quantity",
        F.col("recievedQuantity").cast("double")
    )

    .withColumn(
        "_selling_price",
        F.col("sellingPrice").cast("double")
    )

    .withColumn(
        "_cost_price",
        F.col("costPrice").cast("double")
    )

    .withColumn(
        "_total_cost",
        F.col("totalCost").cast("double")
    )

    .withColumn(
        "_gst",
        F.col("gst").cast("double")
    )

    .withColumn(
        "_gst_amount",
        F.col("gstAmount").cast("double")
    )

    .withColumn(
        "_cess",
        F.col("cess").cast("double")
    )

    .withColumn(
        "_cess_amount",
        F.col("cessAmount").cast("double")
    )

    .withColumn(
        "_access",
        F.col("acess").cast("double")
    )

    .withColumn(
        "_access_amount",
        F.col("acessAmount").cast("double")
    )

    .withColumn(
        "_total_taxes",
        F.col("totalTaxes").cast("double")
    )

    .withColumn(
        "_amount_net",
        F.col("amountNet").cast("double")
    )
)


# ============================================================
# 7. SOURCE IDENTITY VALIDATION
# ============================================================

print("\n[5/18] Validating GRN source identity...")

invalid_identity = (

    grn

    .filter(

        F.col("grn_date").isNull()

        |

        F.col("_barcode").isNull()

        |

        (F.col("_barcode") == "")

        |

        F.col("_store_name_normalized").isNull()

        |

        (F.col("_store_name_normalized") == "")

    )
)

invalid_identity_count = (
    invalid_identity.count()
)

print(
    f"Rows with invalid GRN identity: "
    f"{invalid_identity_count:,}"
)

if invalid_identity_count > 0:

    print(
        "\nSample invalid rows:"
    )

    (
        invalid_identity

        .select(
            "Date",
            "barcode",
            "productName",
            "storeName",
            "grnId",
            "invoiceNumber"
        )

        .show(
            20,
            truncate=False
        )
    )

    raise RuntimeError(
        f"❌ {invalid_identity_count:,} GRN rows have "
        f"invalid date/barcode/store identity."
    )

print(
    "✅ GRN source identity validated"
)


# ============================================================
# 8. LOAD DIMENSIONS
# ============================================================

print("\n[6/18] Loading dimensions...")

# ------------------------------------------------------------
# PRODUCT DIMENSION
# ------------------------------------------------------------

dim_product = (

    spark.table(PRODUCT_DIM)

    .select(
        "product_key",
        "barcode",
        "product_name",
        "brand_name",
        "category_name",
        "hsn_code"
    )

    .withColumn(
        "_barcode",
        normalize_barcode(
            F.col("barcode")
        )
    )
)


# ------------------------------------------------------------
# STORE DIMENSION
# ------------------------------------------------------------

dim_store = (

    spark.table(STORE_DIM)

    .select(
        "store_key",
        "store_id",
        "store_name",
        "normalized_store_name"
    )

    .withColumn(
        "_store_name_normalized",
        normalize_store_name(
            F.col("store_name")
        )
    )
)


# ------------------------------------------------------------
# DATE DIMENSION
#
# IMPORTANT:
# dim_date uses `date`, NOT `calendar_date`.
# ------------------------------------------------------------

dim_date = (

    spark.table(DATE_DIM)

    .select(
        F.col("date_key")
        .cast("int")
        .alias("date_key"),

        F.col("date")
        .cast("date")
        .alias("date")
    )
)


print(
    f"dim_product rows: "
    f"{dim_product.count():,}"
)

print(
    f"dim_store rows: "
    f"{dim_store.count():,}"
)

print(
    f"dim_date rows: "
    f"{dim_date.count():,}"
)

print(
    f"dim_date columns: "
    f"{dim_date.columns}"
)


# ============================================================
# 9. VALIDATE DIMENSION IDENTITIES
# ============================================================

print(
    "\n[7/18] Validating dimension identities..."
)


# ------------------------------------------------------------
# PRODUCT BARCODE UNIQUENESS
# ------------------------------------------------------------

product_barcode_duplicates = (

    dim_product

    .filter(
        F.col("_barcode").isNotNull()
        &
        (F.col("_barcode") != "")
    )

    .groupBy("_barcode")

    .count()

    .filter(
        F.col("count") > 1
    )
)

product_barcode_duplicate_count = (
    product_barcode_duplicates.count()
)

if product_barcode_duplicate_count > 0:

    print(
        "\n❌ Duplicate product barcodes detected:"
    )

    product_barcode_duplicates.show(
        20,
        truncate=False
    )

    raise RuntimeError(
        f"❌ dim_product contains "
        f"{product_barcode_duplicate_count:,} "
        f"duplicate barcode identities."
    )

print(
    "✅ dim_product barcode identity is unique"
)


# ------------------------------------------------------------
# STORE NORMALIZED NAME UNIQUENESS
# ------------------------------------------------------------

store_name_duplicates = (

    dim_store

    .groupBy(
        "_store_name_normalized"
    )

    .count()

    .filter(
        F.col("count") > 1
    )
)

store_name_duplicate_count = (
    store_name_duplicates.count()
)

if store_name_duplicate_count > 0:

    print(
        "\n❌ Duplicate normalized store names detected:"
    )

    store_name_duplicates.show(
        20,
        truncate=False
    )

    raise RuntimeError(
        f"❌ dim_store contains "
        f"{store_name_duplicate_count:,} "
        f"duplicate normalized store names."
    )

print(
    "✅ dim_store normalized-name identity is unique"
)


# ============================================================
# 10. STORE LOOKUP
# ============================================================

print(
    "\n[8/18] Joining GRN to store dimension..."
)

grn_store = (

    grn.alias("g")

    .join(

        dim_store.alias("s"),

        F.col("g._store_name_normalized")
        ==
        F.col("s._store_name_normalized"),

        "left"

    )

    .select(

        "g.*",

        F.col("s.store_key")
        .alias("store_key"),

        F.col("s.store_id")
        .alias("store_id"),

        F.col("s.store_name")
        .alias("canonical_store_name")

    )
)


# ============================================================
# 11. DIAGNOSE AND SKIP UNMATCHED STORES
# ============================================================

unmatched_store_rows = (

    grn_store

    .filter(
        F.col("store_key").isNull()
    )
)

unmatched_store_count = (
    unmatched_store_rows.count()
)

print(
    f"Unmatched GRN store rows: "
    f"{unmatched_store_count:,}"
)


if unmatched_store_count > 0:

    print(
        "\n⚠️ WARNING: "
        f"{unmatched_store_count:,} GRN rows "
        "belong to stores not present in dim_store."
    )

    print(
        "\nUnmatched stores:"
    )

    (

        unmatched_store_rows

        .groupBy(
            "_store_name_normalized"
        )

        .count()

        .orderBy(
            F.desc("count")
        )

        .show(
            100,
            truncate=False
        )
    )

    print(
        "\n⚠️ These unmatched-store GRN rows "
        "will be SKIPPED."
    )

    print(
        "They will NOT be inserted into fact_grn."
    )


# ------------------------------------------------------------
# KEEP ONLY STORES THAT EXIST IN dim_store
# ------------------------------------------------------------

grn_store = (

    grn_store

    .filter(
        F.col("store_key").isNotNull()
    )
)


matched_store_count = (
    grn_store.count()
)

print(
    f"\nMatched GRN store rows retained: "
    f"{matched_store_count:,}"
)

print(
    f"Unmatched GRN store rows skipped: "
    f"{unmatched_store_count:,}"
)


if matched_store_count == 0:

    raise RuntimeError(
        "❌ No GRN rows matched dim_store. "
        "fact_grn cannot be built."
    )

print(
    "✅ Store filtering completed"
)

# ============================================================
# 12. PRODUCT LOOKUP
# ============================================================

print(
    "\n[9/18] Joining GRN to product dimension..."
)

grn_product = (

    grn_store.alias("g")

    .join(

        dim_product.alias("p"),

        F.col("g._barcode")
        ==
        F.col("p._barcode"),

        "left"

    )

    .select(

        "g.*",

        F.col("p.product_key")
        .alias("product_key"),

        F.col("p.product_name")
        .alias("canonical_product_name"),

        F.col("p.brand_name")
        .alias("canonical_brand_name"),

        F.col("p.category_name")
        .alias("canonical_category_name"),

        F.col("p.hsn_code")
        .alias("canonical_hsn_code")

    )
)


# ============================================================
# 13. DIAGNOSE UNMATCHED PRODUCTS
# ============================================================

print(
    "\n[10/18] Checking GRN product barcode matches..."
)

unmatched_product_rows = (

    grn_product

    .filter(
        F.col("product_key").isNull()
    )
)

unmatched_product_count = (
    unmatched_product_rows.count()
)

unmatched_product_barcode_count = (

    unmatched_product_rows

    .select("_barcode")

    .distinct()

    .count()
)

print(
    f"Unmatched GRN product rows: "
    f"{unmatched_product_count:,}"
)

print(
    f"Distinct unmatched GRN barcodes: "
    f"{unmatched_product_barcode_count:,}"
)


if unmatched_product_count > 0:

    print(
        "\n⚠️ WARNING: "
        "Unmatched GRN product barcodes found."
    )

    print(
        "\nSample unmatched GRN records:"
    )

    (

        unmatched_product_rows

        .select(

            "_barcode",
            "productName",
            "brandName",
            "categoryName",
            "storeName",
            "grn_date",
            "grnId",
            "invoiceNumber",
            "recievedQuantity",
            "totalCost",
            "amountNet"

        )

        .orderBy("_barcode")

        .show(
            50,
            truncate=False
        )
    )

    print(
        "\nSample unmatched barcode list:"
    )

    (

        unmatched_product_rows

        .select("_barcode")

        .distinct()

        .orderBy("_barcode")

        .show(
            50,
            truncate=False
        )
    )

    print(
        "\n⚠️ These rows will NOT be inserted "
        "into fact_grn."
    )

    print(
        "⚠️ They should be investigated "
        "against dim_product."
    )


# ============================================================
# 14. REMOVE UNMATCHED PRODUCTS
# ============================================================

print(
    "\n[11/18] Filtering unmatched products..."
)

matched_grn = (

    grn_product

    .filter(
        F.col("product_key").isNotNull()
    )
)

matched_product_count = (
    matched_grn.count()
)

print(
    f"Matched GRN product rows: "
    f"{matched_product_count:,}"
)

if matched_product_count == 0:

    raise RuntimeError(
        "❌ No GRN rows matched dim_product. "
        "fact_grn cannot be built."
    )


# ============================================================
# 15. DATE LOOKUP
# ============================================================

print(
    "\n[12/18] Joining GRN to date dimension..."
)

# ------------------------------------------------------------
# IMPORTANT:
#
# dim_date actual schema:
#
#   date_key
#   date
#
# There is NO calendar_date column.
# ------------------------------------------------------------

grn_dim = (

    matched_grn.alias("g")

    .join(

        dim_date.alias("d"),

        F.col("g.grn_date")
        ==
        F.col("d.date"),

        "left"

    )

    .select(

        F.col("g.*"),

        F.col("d.date_key")
        .alias("date_key")

    )
)


unmatched_date_rows = (

    grn_dim

    .filter(
        F.col("date_key").isNull()
    )
)

unmatched_date_count = (
    unmatched_date_rows.count()
)

print(
    f"Unmatched GRN date rows: "
    f"{unmatched_date_count:,}"
)

if unmatched_date_count > 0:

    print(
        "\nUnmatched GRN dates:"
    )

    (

        unmatched_date_rows

        .groupBy(
            "grn_date"
        )

        .count()

        .orderBy(
            "grn_date"
        )

        .show(
            50,
            truncate=False
        )
    )

    raise RuntimeError(
        f"❌ {unmatched_date_count:,} GRN rows "
        f"have unmatched dates in dim_date."
    )

print(
    "✅ All GRN dates matched dim_date"
)


# ============================================================
# 16. CHECK SOURCE GRAIN DUPLICATES
# ============================================================

print(
    "\n[13/18] Checking source grain..."
)

SOURCE_GRAIN = [

    "product_key",
    "store_key",
    "grn_date"

]


source_grain_counts = (

    grn_dim

    .groupBy(
        *SOURCE_GRAIN
    )

    .count()
)


duplicate_source_grain = (

    source_grain_counts

    .filter(
        F.col("count") > 1
    )
)


duplicate_source_grain_count = (
    duplicate_source_grain.count()
)


additional_duplicate_rows = (

    duplicate_source_grain

    .select(

        F.sum(
            F.col("count") - 1
        )

        .alias("additional_rows")

    )

    .collect()[0]["additional_rows"]

    or 0

)


print(
    f"Duplicate source grain combinations: "
    f"{duplicate_source_grain_count:,}"
)

print(
    f"Additional rows caused by source duplicates: "
    f"{additional_duplicate_rows:,}"
)


if duplicate_source_grain_count > 0:

    print(
        "\nSample duplicate source grain:"
    )

    (

        duplicate_source_grain

        .orderBy(
            F.desc("count")
        )

        .show(
            20,
            truncate=False
        )
    )


# ============================================================
# 17. BUILD FACT GRN
# ============================================================

print(
    "\n[14/18] Building fact_grn..."
)

fact_grn = (

    grn_dim

    .groupBy(

        "product_key",
        "store_key",
        "date_key",
        "grn_date"

    )

    .agg(

        # ----------------------------------------------------
        # ADDITIVE MEASURES
        # ----------------------------------------------------

        F.sum(
            F.coalesce(
                F.col("_received_quantity"),
                F.lit(0.0)
            )
        ).alias(
            "received_quantity"
        ),

        F.sum(
            F.coalesce(
                F.col("_total_cost"),
                F.lit(0.0)
            )
        ).alias(
            "total_cost"
        ),

        F.sum(
            F.coalesce(
                F.col("_gst_amount"),
                F.lit(0.0)
            )
        ).alias(
            "gst_amount"
        ),

        F.sum(
            F.coalesce(
                F.col("_cess_amount"),
                F.lit(0.0)
            )
        ).alias(
            "cess_amount"
        ),

        F.sum(
            F.coalesce(
                F.col("_access_amount"),
                F.lit(0.0)
            )
        ).alias(
            "access_amount"
        ),

        F.sum(
            F.coalesce(
                F.col("_total_taxes"),
                F.lit(0.0)
            )
        ).alias(
            "total_taxes"
        ),

        F.sum(
            F.coalesce(
                F.col("_amount_net"),
                F.lit(0.0)
            )
        ).alias(
            "amount_net"
        ),


        # ----------------------------------------------------
        # UNIT / RATE FIELDS
        # ----------------------------------------------------

        F.max(
            F.col("_selling_price")
        ).alias(
            "selling_price"
        ),

        F.max(
            F.col("_cost_price")
        ).alias(
            "cost_price"
        ),

        F.max(
            F.col("_gst")
        ).alias(
            "gst"
        ),

        F.max(
            F.col("_cess")
        ).alias(
            "cess"
        ),

        F.max(
            F.col("_access")
        ).alias(
            "access"
        ),


        # ----------------------------------------------------
        # LINEAGE / DESCRIPTIVE FIELDS
        # ----------------------------------------------------

        F.first(
            F.col("grnId"),
            ignorenulls=True
        ).alias(
            "sample_grn_id"
        ),

        F.first(
            F.col("invoiceNumber"),
            ignorenulls=True
        ).alias(
            "sample_invoice_number"
        ),

        F.first(
            F.col("Time"),
            ignorenulls=True
        ).alias(
            "sample_time"
        ),

        F.first(
            F.col("productName"),
            ignorenulls=True
        ).alias(
            "source_product_name"
        ),

        F.first(
            F.col("brandName"),
            ignorenulls=True
        ).alias(
            "source_brand_name"
        ),

        F.first(
            F.col("categoryName"),
            ignorenulls=True
        ).alias(
            "source_category_name"
        ),

        F.first(
            F.col("vendor"),
            ignorenulls=True
        ).alias(
            "vendor"
        ),

        F.first(
            F.col("vendorGst"),
            ignorenulls=True
        ).alias(
            "vendor_gst"
        ),

        F.first(
            F.col("user"),
            ignorenulls=True
        ).alias(
            "created_by"
        ),

        F.first(
            F.col("canonical_product_name"),
            ignorenulls=True
        ).alias(
            "product_name"
        ),

        F.first(
            F.col("canonical_brand_name"),
            ignorenulls=True
        ).alias(
            "brand_name"
        ),

        F.first(
            F.col("canonical_category_name"),
            ignorenulls=True
        ).alias(
            "category_name"
        ),

        F.first(
            F.col("canonical_hsn_code"),
            ignorenulls=True
        ).alias(
            "hsn_code"
        ),

        F.first(
            F.col("canonical_store_name"),
            ignorenulls=True
        ).alias(
            "store_name"
        )

    )
)


# ============================================================
# 18. ROUND / CLEAN MEASURES
# ============================================================

fact_grn = (

    fact_grn

    .withColumn(
        "received_quantity",
        F.round(
            F.col("received_quantity"),
            3
        )
    )

    .withColumn(
        "selling_price",
        F.round(
            F.col("selling_price"),
            2
        )
    )

    .withColumn(
        "cost_price",
        F.round(
            F.col("cost_price"),
            2
        )
    )

    .withColumn(
        "total_cost",
        F.round(
            F.col("total_cost"),
            2
        )
    )

    .withColumn(
        "gst",
        F.round(
            F.col("gst"),
            4
        )
    )

    .withColumn(
        "gst_amount",
        F.round(
            F.col("gst_amount"),
            2
        )
    )

    .withColumn(
        "cess",
        F.round(
            F.col("cess"),
            4
        )
    )

    .withColumn(
        "cess_amount",
        F.round(
            F.col("cess_amount"),
            2
        )
    )

    .withColumn(
        "access",
        F.round(
            F.col("access"),
            4
        )
    )

    .withColumn(
        "access_amount",
        F.round(
            F.col("access_amount"),
            2
        )
    )

    .withColumn(
        "total_taxes",
        F.round(
            F.col("total_taxes"),
            2
        )
    )

    .withColumn(
        "amount_net",
        F.round(
            F.col("amount_net"),
            2
        )
    )

    .withColumn(
        "processed_at",
        F.current_timestamp()
    )
)


# ============================================================
# 19. FACT GRAIN VALIDATION
# ============================================================

print(
    "\n[15/18] Validating final fact grain..."
)

FINAL_GRAIN = [

    "product_key",
    "store_key",
    "date_key",
    "grn_date"

]


assert_no_duplicate_grain(

    fact_grn,

    FINAL_GRAIN,

    "fact_grn"

)


# ============================================================
# 20. PRE-WRITE METRICS
# ============================================================

fact_grn_count = (
    fact_grn.count()
)


fact_grn_store_count = (

    fact_grn

    .select(
        "store_key"
    )

    .distinct()

    .count()

)


fact_grn_product_count = (

    fact_grn

    .select(
        "product_key"
    )

    .distinct()

    .count()

)


fact_grn_date_count = (

    fact_grn

    .select(
        "grn_date"
    )

    .distinct()

    .count()

)


fact_grn_received_quantity = (

    fact_grn

    .agg(
        F.sum(
            "received_quantity"
        ).alias("total")
    )

    .collect()[0]["total"]

    or 0

)


fact_grn_total_cost = (

    fact_grn

    .agg(
        F.sum(
            "total_cost"
        ).alias("total")
    )

    .collect()[0]["total"]

    or 0

)


fact_grn_amount_net = (

    fact_grn

    .agg(
        F.sum(
            "amount_net"
        ).alias("total")
    )

    .collect()[0]["total"]

    or 0

)


print(
    f"Final fact rows: "
    f"{fact_grn_count:,}"
)

print(
    f"Stores represented: "
    f"{fact_grn_store_count:,}"
)

print(
    f"Products represented: "
    f"{fact_grn_product_count:,}"
)

print(
    f"Dates represented: "
    f"{fact_grn_date_count:,}"
)

print(
    f"Received quantity: "
    f"{fact_grn_received_quantity:,.2f}"
)

print(
    f"Total cost: "
    f"₹{fact_grn_total_cost:,.2f}"
)

print(
    f"Amount net: "
    f"₹{fact_grn_amount_net:,.2f}"
)


# ============================================================
# 21. SOURCE → FACT RECONCILIATION
# ============================================================

print(
    "\n[16/18] Performing source → fact reconciliation..."
)

accepted_grn_count = (
    grn_dim.count()
)

print(
    f"Accepted GRN rows after identity validation: "
    f"{accepted_grn_count:,}"
)

print(
    f"Unmatched product rows excluded: "
    f"{unmatched_product_count:,}"
)

print(
    f"Matched product rows entering fact build: "
    f"{matched_product_count:,}"
)

print(
    f"Fact rows after consolidation: "
    f"{fact_grn_count:,}"
)


expected_consolidated_rows = (

    matched_product_count
    -
    fact_grn_count

)


print(
    f"Rows consolidated due to duplicate source grain: "
    f"{expected_consolidated_rows:,}"
)


if (
    expected_consolidated_rows
    !=
    additional_duplicate_rows
):

    raise RuntimeError(

        "❌ GRN reconciliation failed.\n"

        f"Expected consolidated rows: "
        f"{expected_consolidated_rows:,}\n"

        f"Observed duplicate-source rows: "
        f"{additional_duplicate_rows:,}"

    )


print(
    "✅ Source → fact row reconciliation passed"
)


# ============================================================
# 22. DATE RANGE
# ============================================================

print(
    "\n[17/18] Determining GRN date partitions..."
)


date_rows = (

    fact_grn

    .select(
        "grn_date"
    )

    .distinct()

    .orderBy(
        "grn_date"
    )

    .collect()

)


grn_dates = [

    row["grn_date"]

    for row in date_rows

]


if not grn_dates:

    raise RuntimeError(
        "❌ fact_grn contains no dates."
    )


print(
    f"GRN date range: "
    f"{min(grn_dates)} → {max(grn_dates)}"
)

print(
    f"Partitions to replace: "
    f"{len(grn_dates):,}"
)


# ============================================================
# 23. IDEMPOTENT WRITE
# ============================================================

print(
    "\n[18/18] Writing fact_grn idempotently..."
)


TARGET_COLUMNS = [

    "product_key",
    "store_key",
    "date_key",
    "grn_date",

    "received_quantity",
    "selling_price",
    "cost_price",
    "total_cost",

    "gst",
    "gst_amount",

    "cess",
    "cess_amount",

    "access",
    "access_amount",

    "total_taxes",
    "amount_net",

    "sample_grn_id",
    "sample_invoice_number",
    "sample_time",

    "source_product_name",
    "source_brand_name",
    "source_category_name",

    "vendor",
    "vendor_gst",
    "created_by",

    "product_name",
    "brand_name",
    "category_name",
    "hsn_code",
    "store_name",

    "processed_at"

]


fact_grn = (
    fact_grn
    .select(*TARGET_COLUMNS)
)


# ------------------------------------------------------------
# CREATE TARGET IF IT DOES NOT EXIST
# ------------------------------------------------------------

if not spark.catalog.tableExists(
    TARGET_TABLE
):

    print(
        f"Target table does not exist. "
        f"Creating {TARGET_TABLE}..."
    )

    (

        fact_grn

        .write

        .format("delta")

        .mode("overwrite")

        .partitionBy(
            "grn_date"
        )

        .saveAsTable(
            TARGET_TABLE
        )

    )

else:

    print(
        f"Target table exists. "
        f"Replacing affected GRN date partitions..."
    )


    target_delta = (
        DeltaTable.forName(
            spark,
            TARGET_TABLE
        )
    )


    # --------------------------------------------------------
    # Delete only affected dates
    # --------------------------------------------------------

    target_delta.delete(
        F.col("grn_date").isin(
            grn_dates
        )
    )


    # --------------------------------------------------------
    # Append rebuilt partitions
    # --------------------------------------------------------

    (

        fact_grn

        .write

        .format("delta")

        .mode("append")

        .saveAsTable(
            TARGET_TABLE
        )

    )


# ============================================================
# 24. POST-WRITE VALIDATION
# ============================================================

print(
    "\n" + "=" * 80
)

print(
    "POST-WRITE VALIDATION"
)

print(
    "=" * 80
)


target_df = (
    spark.table(
        TARGET_TABLE
    )
)


# ------------------------------------------------------------
# Validate affected dates
# ------------------------------------------------------------

written_df = (

    target_df

    .filter(
        F.col("grn_date").isin(
            grn_dates
        )
    )

)


written_count = (
    written_df.count()
)


print(
    f"\nRows written for affected dates: "
    f"{written_count:,}"
)


if written_count != fact_grn_count:

    raise RuntimeError(

        "❌ Post-write row-count validation failed.\n"

        f"Expected: "
        f"{fact_grn_count:,}\n"

        f"Found: "
        f"{written_count:,}"

    )


print(
    "✅ Post-write row count passed"
)


# ------------------------------------------------------------
# Validate duplicate grain
# ------------------------------------------------------------

assert_no_duplicate_grain(

    written_df,

    FINAL_GRAIN,

    "post-write fact_grn"

)


# ------------------------------------------------------------
# Validate null keys
# ------------------------------------------------------------

null_key_count = (

    written_df

    .filter(

        F.col("product_key").isNull()

        |

        F.col("store_key").isNull()

        |

        F.col("date_key").isNull()

        |

        F.col("grn_date").isNull()

    )

    .count()

)


if null_key_count > 0:

    raise RuntimeError(

        f"❌ {null_key_count:,} fact_grn rows "
        f"contain NULL dimension keys."

    )


print(
    "✅ Dimension-key null validation passed"
)


# ------------------------------------------------------------
# Validate unmatched stores
# ------------------------------------------------------------

unmatched_written_stores = (

    written_df

    .filter(
        F.col("store_key").isNull()
    )

    .count()

)


if unmatched_written_stores > 0:

    raise RuntimeError(

        f"❌ {unmatched_written_stores:,} rows have "
        f"unmatched stores after write."

    )


print(
    "✅ Store-key validation passed"
)


# ------------------------------------------------------------
# Validate unmatched products
# ------------------------------------------------------------

unmatched_written_products = (

    written_df

    .filter(
        F.col("product_key").isNull()
    )

    .count()

)


if unmatched_written_products > 0:

    raise RuntimeError(

        f"❌ {unmatched_written_products:,} rows have "
        f"unmatched products after write."

    )


print(
    "✅ Product-key validation passed"
)


# ============================================================
# 25. FINAL SUMMARY
# ============================================================

print(
    "\n" + "=" * 80
)

print(
    "24_FACT_GRN COMPLETED SUCCESSFULLY"
)

print(
    "=" * 80
)


print(

    f"""

SOURCE
------
Raw GRN rows                  : {raw_grn_count:,}
Invalid identity rows         : {invalid_identity_count:,}
Unmatched store rows          : {unmatched_store_count:,}
Unmatched product rows        : {unmatched_product_count:,}
Distinct unmatched barcodes  : {unmatched_product_barcode_count:,}


FACT
----
Matched product rows          : {matched_product_count:,}
Fact rows                     : {fact_grn_count:,}
Source duplicate grain groups : {duplicate_source_grain_count:,}
Rows consolidated             : {expected_consolidated_rows:,}


COVERAGE
--------
Stores                        : {fact_grn_store_count:,}
Products                      : {fact_grn_product_count:,}
Dates                         : {fact_grn_date_count:,}


MEASURES
--------
Received quantity             : {fact_grn_received_quantity:,.2f}
Total cost                    : ₹{fact_grn_total_cost:,.2f}
Amount net                    : ₹{fact_grn_amount_net:,.2f}


DATE RANGE
----------
From                          : {min(grn_dates)}
To                            : {max(grn_dates)}


TARGET
------
{TARGET_TABLE}


STATUS
------
✅ Idempotent write completed
✅ Store normalization applied
✅ Product barcode normalization applied
✅ Date dimension validated
✅ Duplicate grain consolidated
✅ Post-write validation passed

"""
)


# ============================================================
# 26. DATA QUALITY WARNING
# ============================================================

if unmatched_product_count > 0:

    print(
        "\n" + "!" * 80
    )

    print(
        "IMPORTANT DATA QUALITY WARNING"
    )

    print(
        "!" * 80
    )

    print(

        f"""

{unmatched_product_count:,} GRN rows were excluded because their
barcodes were not found in dim_product.

This did NOT fail the pipeline.

These barcodes should be investigated because dim_product
was designed to include product identities from GRN as one
of its source systems.

DO NOT manually insert them into fact_grn.

Investigate:

1. Barcode normalization
2. GRN barcode datatype / format
3. Whether dim_product was rebuilt after the latest GRN data
4. Whether the barcode exists in sales / stock / RTV
5. Whether these are genuinely new products

The unmatched records were printed above.

"""
    )


print(
    "\n✅ 24_fact_grn finished."
)

# COMMAND ----------

