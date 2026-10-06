# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# Databricks notebook source

# ================================================================================
# 25_SEMANTIC_VIEWS
# ================================================================================
#
# Purpose:
#   Create the business-facing semantic layer over the validated star schema.
#
# IMPORTANT:
#   fact_sales is NOT a surrogate-key fact table.
#   Its actual source columns are:
#
#       invoice
#       storeName
#       productId
#       productName
#       barcode
#       quantity
#       sellingPrice
#       discountAmount
#       totalProductPrice
#       costPrice
#       orderDate
#       orderStatus
#       ...
#
#   Therefore:
#
#       fact_sales.storeName -> dim_store
#       fact_sales.barcode   -> dim_product
#       fact_sales.orderDate -> dim_date
#
#   The semantic view derives store_key/product_key/date_key from these joins.
#
# Business rules:
#   1. Sales reporting does NOT filter orderStatus = 'PLACED'
#   2. Ho Marlboro is excluded from sales reporting
#   3. Store identity comes from dim_store
#   4. Product identity comes from dim_product
#   5. Date identity comes from dim_date
#   6. Views are read-only semantic objects
#   7. No physical fact tables are modified
#   8. Store names are normalized before matching
#   9. Product identity is barcode-based
#
# ================================================================================

from pyspark.sql import functions as F


# ================================================================================
# 0. CONFIGURATION
# ================================================================================

CATALOG = "billing"
SCHEMA = "sales_data"

BASE = f"{CATALOG}.{SCHEMA}"

FACT_SALES = f"{BASE}.fact_sales"
FACT_STOCK = f"{BASE}.fact_stock"
FACT_RTV = f"{BASE}.fact_rtv"
FACT_GRN = f"{BASE}.fact_grn"

DIM_STORE = f"{BASE}.dim_store"
DIM_PRODUCT = f"{BASE}.dim_product"
DIM_BRAND = f"{BASE}.dim_brand"
DIM_CATEGORY = f"{BASE}.dim_category"
DIM_VENDOR = f"{BASE}.dim_vendor"
DIM_DATE = f"{BASE}.dim_date"


# ================================================================================
# 1. REQUIRED TABLE VALIDATION
# ================================================================================

print("=" * 80)
print("25 SEMANTIC VIEWS")
print("=" * 80)

required_tables = [
    FACT_SALES,
    FACT_STOCK,
    FACT_RTV,
    FACT_GRN,
    DIM_STORE,
    DIM_PRODUCT,
    DIM_BRAND,
    DIM_CATEGORY,
    DIM_VENDOR,
    DIM_DATE,
]

print("\n[1/12] Validating required tables...")

missing_tables = []

for table_name in required_tables:

    if not spark.catalog.tableExists(table_name):
        missing_tables.append(table_name)
        print(f"❌ Missing: {table_name}")

    else:
        print(f"✅ {table_name}")


if missing_tables:

    raise RuntimeError(
        "Required tables are missing:\n"
        + "\n".join(missing_tables)
    )


print("✅ All required tables exist")


# ================================================================================
# 2. HELPER FUNCTIONS
# ================================================================================

def get_columns(table_name):
    return set(spark.table(table_name).columns)


def require_columns(table_name, required_columns):

    actual = get_columns(table_name)

    missing = [
        column
        for column in required_columns
        if column not in actual
    ]

    if missing:

        raise RuntimeError(
            f"\n❌ Missing columns in {table_name}:\n"
            + "\n".join(missing)
            + "\n\nAvailable columns:\n"
            + "\n".join(sorted(actual))
        )


def create_or_replace_view(view_name, sql_text):

    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {view_name} AS
        {sql_text}
        """
    )

    print(f"✅ Created: {view_name}")


def normalize_store_name_sql(column_expression):

    return f"""
        LOWER(
            TRIM(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        CAST({column_expression} AS STRING),
                        '[\\\\t\\\\r\\\\n\\\\u00A0]+',
                        ' '
                    ),
                    '\\\\s+',
                    ' '
                )
            )
        )
    """


# ================================================================================
# 3. VALIDATE ACTUAL SCHEMAS
# ================================================================================

print("\n[2/12] Validating actual source schemas...")


# ------------------------------------------------------------------------------
# FACT SALES
# ------------------------------------------------------------------------------

require_columns(
    FACT_SALES,
    [
        "invoice",
        "storeName",
        "barcode",
        "quantity",
        "sellingPrice",
        "discountAmount",
        "totalProductPrice",
        "costPrice",
        "orderDate",
        "orderStatus",
    ],
)


# ------------------------------------------------------------------------------
# FACT STOCK
# ------------------------------------------------------------------------------

require_columns(
    FACT_STOCK,
    [
        "store_key",
        "product_key",
        "date_key",
        "snapshot_date",
        "quantity",
        "selling_price",
        "printed_mrp",
        "cost_price",
        "total_amount",
    ],
)


# ------------------------------------------------------------------------------
# FACT RTV
# ------------------------------------------------------------------------------

require_columns(
    FACT_RTV,
    [
        "store_key",
        "product_key",
        "date_key",
        "rtv_date",
        "quantity",
        "total_amount",
    ],
)


# ------------------------------------------------------------------------------
# FACT GRN
# ------------------------------------------------------------------------------

require_columns(
    FACT_GRN,
    [
        "store_key",
        "product_key",
        "date_key",
        "grn_date",
        "received_quantity",
        "total_cost",
        "amount_net",
    ],
)


# ------------------------------------------------------------------------------
# DIM STORE
# ------------------------------------------------------------------------------

require_columns(
    DIM_STORE,
    [
        "store_key",
        "store_id",
        "store_name",
        "normalized_store_name",
    ],
)


# ------------------------------------------------------------------------------
# DIM PRODUCT
# ------------------------------------------------------------------------------

require_columns(
    DIM_PRODUCT,
    [
        "product_key",
        "barcode",
        "product_name",
        "brand_name",
        "category_name",
    ],
)


# ------------------------------------------------------------------------------
# DIM DATE
# ------------------------------------------------------------------------------

require_columns(
    DIM_DATE,
    [
        "date_key",
        "date",
        "year",
        "quarter",
        "quarter_name",
        "quarter_number",
        "month",
        "month_number",
        "month_name",
        "month_short_name",
        "year_month",
        "year_month_key",
        "day_of_month",
        "day_of_year",
        "day_of_week",
        "day_name",
        "day_short_name",
        "iso_day_of_week",
        "week_of_year",
        "year_week",
        "week_start_date",
        "week_end_date",
        "month_start_date",
        "month_end_date",
        "days_in_month",
        "month_day_number",
        "quarter_start_date",
        "quarter_end_date",
        "year_start_date",
        "year_end_date",
        "is_weekend",
        "is_weekday",
    ],
)


print("✅ Actual schemas validated")


# ================================================================================
# 4. DIMENSION SEMANTIC VIEWS
# ================================================================================

print("\n[3/12] Creating dimension semantic views...")


# ------------------------------------------------------------------------------
# STORE
# ------------------------------------------------------------------------------

create_or_replace_view(
    f"{BASE}.vw_dim_store",
    f"""
    SELECT
        store_key,
        store_id,
        store_name,
        normalized_store_name

    FROM {DIM_STORE}
    """
)


# ------------------------------------------------------------------------------
# PRODUCT
# ------------------------------------------------------------------------------

create_or_replace_view(
    f"{BASE}.vw_dim_product",
    f"""
    SELECT
        product_key,
        barcode,
        product_name,
        brand_name,
        category_name

    FROM {DIM_PRODUCT}
    """
)


# ------------------------------------------------------------------------------
# DATE
# ------------------------------------------------------------------------------

create_or_replace_view(
    f"{BASE}.vw_dim_date",
    f"""
    SELECT
        date_key,
        date,

        year,
        quarter,
        quarter_name,
        quarter_number,

        month,
        month_number,
        month_name,
        month_short_name,

        year_month,
        year_month_key,

        day_of_month,
        day_of_year,
        day_of_week,
        day_name,
        day_short_name,
        iso_day_of_week,

        week_of_year,
        year_week,

        week_start_date,
        week_end_date,

        month_start_date,
        month_end_date,

        days_in_month,
        month_day_number,

        quarter_start_date,
        quarter_end_date,

        year_start_date,
        year_end_date,

        is_weekend,
        is_weekday

    FROM {DIM_DATE}
    """
)


print("✅ Dimension views created")


# ================================================================================
# 5. SALES SEMANTIC VIEW
# ================================================================================

print("\n[4/12] Creating sales semantic view...")


# ------------------------------------------------------------------------------
# SALES NORMALIZATION
#
# fact_sales does NOT contain surrogate keys.
#
# We derive:
#
#   store_key   -> normalized storeName
#   product_key -> barcode
#   date_key    -> orderDate
#
# ------------------------------------------------------------------------------

create_or_replace_view(
    f"{BASE}.vw_sales",
    f"""
    WITH sales_normalized AS (

        SELECT

            fs.invoice,

            fs.storeName,
            fs.barcode,

            fs.quantity,
            fs.sellingPrice,
            fs.discountAmount,
            fs.totalProductPrice,
            fs.costPrice,

            CAST(fs.orderDate AS DATE) AS order_date,

            fs.orderStatus,

            {normalize_store_name_sql("fs.storeName")}
                AS normalized_store_name,

            CAST(fs.barcode AS STRING)
                AS normalized_barcode

        FROM {FACT_SALES} fs
    ),

    sales_with_dimensions AS (

        SELECT

            ds.store_key,
            dp.product_key,
            dd.date_key,

            s.invoice,

            s.order_date,

            s.orderStatus
                AS order_status,

            s.quantity,

            s.sellingPrice
                AS selling_price,

            s.discountAmount
                AS discount_amount,

            s.totalProductPrice
                AS total_product_price,

            s.costPrice
                AS cost_price,

            ds.store_id,
            ds.store_name,

            dp.barcode,
            dp.product_name,
            dp.brand_name,
            dp.category_name,

            dd.date,

            dd.year,
            dd.quarter,
            dd.quarter_name,
            dd.quarter_number,

            dd.month,
            dd.month_number,
            dd.month_name,
            dd.month_short_name,

            dd.year_month,
            dd.year_month_key,

            dd.week_of_year,
            dd.year_week,

            dd.week_start_date,
            dd.week_end_date,

            dd.month_start_date,
            dd.month_end_date

        FROM sales_normalized s

        INNER JOIN {DIM_STORE} ds

            ON s.normalized_store_name
               =
               ds.normalized_store_name

        INNER JOIN {DIM_PRODUCT} dp

            ON s.normalized_barcode
               =
               CAST(dp.barcode AS STRING)

        INNER JOIN {DIM_DATE} dd

            ON s.order_date
               =
               dd.date
    )

    SELECT *

    FROM sales_with_dimensions

    WHERE LOWER(TRIM(COALESCE(product_name, '')))
          <> 'ho marlboro'
    """
)


print("✅ vw_sales created")


# ================================================================================
# 6. SALES AGGREGATION VIEWS
# ================================================================================

print("\n[5/12] Creating sales aggregation views...")


# ------------------------------------------------------------------------------
# STORE × DAY
# ------------------------------------------------------------------------------

create_or_replace_view(
    f"{BASE}.vw_sales_store_daily",
    f"""
    SELECT

        store_key,
        store_id,
        store_name,

        date_key,
        date,

        year,
        month,
        month_name,
        year_month,

        week_of_year,
        year_week,

        SUM(quantity)
            AS quantity_sold,

        SUM(total_product_price)
            AS sales_amount,

        COUNT(DISTINCT invoice)
            AS invoice_count,

        CASE

            WHEN COUNT(DISTINCT invoice) = 0

                THEN 0.0

            ELSE
                SUM(total_product_price)
                /
                COUNT(DISTINCT invoice)

        END AS average_order_value,

        SUM(discount_amount)
            AS discount_amount,

        SUM(
            cost_price * quantity
        ) AS estimated_cost_amount

    FROM {BASE}.vw_sales

    GROUP BY

        store_key,
        store_id,
        store_name,

        date_key,
        date,

        year,
        month,
        month_name,
        year_month,

        week_of_year,
        year_week
    """
)


# ------------------------------------------------------------------------------
# PRODUCT × STORE × DAY
# ------------------------------------------------------------------------------

create_or_replace_view(
    f"{BASE}.vw_sales_product_daily",
    f"""
    SELECT

        store_key,
        store_id,
        store_name,

        product_key,
        barcode,
        product_name,

        brand_name,
        category_name,

        date_key,
        date,

        year,
        month,
        month_name,
        year_month,

        SUM(quantity)
            AS quantity_sold,

        SUM(total_product_price)
            AS sales_amount,

        COUNT(DISTINCT invoice)
            AS invoice_count,

        SUM(discount_amount)
            AS discount_amount,

        SUM(
            cost_price * quantity
        ) AS estimated_cost_amount

    FROM {BASE}.vw_sales

    GROUP BY

        store_key,
        store_id,
        store_name,

        product_key,
        barcode,
        product_name,

        brand_name,
        category_name,

        date_key,
        date,

        year,
        month,
        month_name,
        year_month
    """
)


# ------------------------------------------------------------------------------
# BRAND × STORE × DAY
# ------------------------------------------------------------------------------

create_or_replace_view(
    f"{BASE}.vw_sales_brand_daily",
    f"""
    SELECT

        store_key,
        store_id,
        store_name,

        date_key,
        date,

        year,
        month,
        month_name,
        year_month,

        brand_name,

        SUM(quantity)
            AS quantity_sold,

        SUM(total_product_price)
            AS sales_amount,

        COUNT(DISTINCT invoice)
            AS invoice_count,

        SUM(discount_amount)
            AS discount_amount

    FROM {BASE}.vw_sales

    GROUP BY

        store_key,
        store_id,
        store_name,

        date_key,
        date,

        year,
        month,
        month_name,
        year_month,

        brand_name
    """
)


# ------------------------------------------------------------------------------
# CATEGORY × STORE × DAY
# ------------------------------------------------------------------------------

create_or_replace_view(
    f"{BASE}.vw_sales_category_daily",
    f"""
    SELECT

        store_key,
        store_id,
        store_name,

        date_key,
        date,

        year,
        month,
        month_name,
        year_month,

        category_name,

        SUM(quantity)
            AS quantity_sold,

        SUM(total_product_price)
            AS sales_amount,

        COUNT(DISTINCT invoice)
            AS invoice_count,

        SUM(discount_amount)
            AS discount_amount

    FROM {BASE}.vw_sales

    GROUP BY

        store_key,
        store_id,
        store_name,

        date_key,
        date,

        year,
        month,
        month_name,
        year_month,

        category_name
    """
)


print("✅ Sales semantic views created")


# ================================================================================
# 7. STOCK SEMANTIC VIEWS
# ================================================================================

print("\n[6/12] Creating stock semantic views...")


# ------------------------------------------------------------------------------
# STOCK DETAIL
# Grain:
#   product × store × snapshot_date
# ------------------------------------------------------------------------------

create_or_replace_view(
    f"{BASE}.vw_stock",
    f"""
    SELECT

        fs.store_key,
        fs.product_key,
        fs.date_key,

        fs.snapshot_date,

        ds.store_id,
        ds.store_name,

        dp.barcode,
        dp.product_name,
        dp.brand_name,
        dp.category_name,

        fs.quantity,
        fs.selling_price,
        fs.printed_mrp,
        fs.cost_price,
        fs.total_amount

    FROM {FACT_STOCK} fs

    INNER JOIN {DIM_STORE} ds

        ON fs.store_key =
           ds.store_key

    INNER JOIN {DIM_PRODUCT} dp

        ON fs.product_key =
           dp.product_key

    INNER JOIN {DIM_DATE} dd

        ON fs.date_key =
           dd.date_key
    """
)


# ------------------------------------------------------------------------------
# LATEST STOCK
#
# One latest snapshot per store/product.
# ------------------------------------------------------------------------------

create_or_replace_view(
    f"{BASE}.vw_stock_latest",
    f"""
    WITH ranked AS (

        SELECT

            s.*,

            ROW_NUMBER() OVER (

                PARTITION BY
                    s.store_key,
                    s.product_key

                ORDER BY
                    s.snapshot_date DESC

            ) AS rn

        FROM {BASE}.vw_stock s
    )

    SELECT

        store_key,
        product_key,
        date_key,

        snapshot_date,

        store_id,
        store_name,

        barcode,
        product_name,
        brand_name,
        category_name,

        quantity,
        selling_price,
        printed_mrp,
        cost_price,
        total_amount

    FROM ranked

    WHERE rn = 1
    """
)


# ------------------------------------------------------------------------------
# STORE × SNAPSHOT
# ------------------------------------------------------------------------------

create_or_replace_view(
    f"{BASE}.vw_stock_store_daily",
    f"""
    SELECT

        store_key,
        store_id,
        store_name,

        date_key,
        snapshot_date,

        SUM(quantity)
            AS stock_quantity,

        SUM(total_amount)
            AS stock_value,

        COUNT(DISTINCT product_key)
            AS product_count

    FROM {BASE}.vw_stock

    GROUP BY

        store_key,
        store_id,
        store_name,

        date_key,
        snapshot_date
    """
)


print("✅ Stock semantic views created")


# ================================================================================
# 8. RTV SEMANTIC VIEWS
# ================================================================================

print("\n[7/12] Creating RTV semantic views...")


# ------------------------------------------------------------------------------
# RTV DETAIL
# ------------------------------------------------------------------------------

create_or_replace_view(
    f"{BASE}.vw_rtv",
    f"""
    SELECT

        fr.store_key,
        fr.product_key,
        fr.date_key,

        fr.rtv_date,

        ds.store_id,
        ds.store_name,

        dp.barcode,
        dp.product_name,
        dp.brand_name,
        dp.category_name,

        fr.quantity,
        fr.total_amount

    FROM {FACT_RTV} fr

    INNER JOIN {DIM_STORE} ds

        ON fr.store_key =
           ds.store_key

    INNER JOIN {DIM_PRODUCT} dp

        ON fr.product_key =
           dp.product_key

    INNER JOIN {DIM_DATE} dd

        ON fr.date_key =
           dd.date_key
    """
)


# ------------------------------------------------------------------------------
# RTV STORE × DAY
# ------------------------------------------------------------------------------

create_or_replace_view(
    f"{BASE}.vw_rtv_store_daily",
    f"""
    SELECT

        store_key,
        store_id,
        store_name,

        date_key,
        rtv_date,

        SUM(quantity)
            AS rtv_quantity,

        SUM(total_amount)
            AS rtv_amount,

        COUNT(DISTINCT product_key)
            AS product_count

    FROM {BASE}.vw_rtv

    GROUP BY

        store_key,
        store_id,
        store_name,

        date_key,
        rtv_date
    """
)


# ------------------------------------------------------------------------------
# RTV PRODUCT × STORE × DAY
# ------------------------------------------------------------------------------

create_or_replace_view(
    f"{BASE}.vw_rtv_product_daily",
    f"""
    SELECT

        store_key,
        store_id,
        store_name,

        product_key,
        barcode,
        product_name,

        brand_name,
        category_name,

        date_key,
        rtv_date,

        SUM(quantity)
            AS rtv_quantity,

        SUM(total_amount)
            AS rtv_amount

    FROM {BASE}.vw_rtv

    GROUP BY

        store_key,
        store_id,
        store_name,

        product_key,
        barcode,
        product_name,

        brand_name,
        category_name,

        date_key,
        rtv_date
    """
)


print("✅ RTV semantic views created")


# ================================================================================
# 9. GRN SEMANTIC VIEWS
# ================================================================================

print("\n[8/12] Creating GRN semantic views...")


# ------------------------------------------------------------------------------
# GRN DETAIL
# ------------------------------------------------------------------------------

create_or_replace_view(
    f"{BASE}.vw_grn",
    f"""
    SELECT

        fg.store_key,
        fg.product_key,
        fg.date_key,

        fg.grn_date,

        ds.store_id,
        ds.store_name,

        dp.barcode,
        dp.product_name,
        dp.brand_name,
        dp.category_name,

        fg.received_quantity,
        fg.total_cost,
        fg.amount_net

    FROM {FACT_GRN} fg

    INNER JOIN {DIM_STORE} ds

        ON fg.store_key =
           ds.store_key

    INNER JOIN {DIM_PRODUCT} dp

        ON fg.product_key =
           dp.product_key

    INNER JOIN {DIM_DATE} dd

        ON fg.date_key =
           dd.date_key
    """
)


# ------------------------------------------------------------------------------
# GRN STORE × DAY
# ------------------------------------------------------------------------------

create_or_replace_view(
    f"{BASE}.vw_grn_store_daily",
    f"""
    SELECT

        store_key,
        store_id,
        store_name,

        date_key,
        grn_date,

        SUM(received_quantity)
            AS received_quantity,

        SUM(total_cost)
            AS purchase_amount,

        SUM(amount_net)
            AS amount_net,

        COUNT(DISTINCT product_key)
            AS product_count

    FROM {BASE}.vw_grn

    GROUP BY

        store_key,
        store_id,
        store_name,

        date_key,
        grn_date
    """
)


# ------------------------------------------------------------------------------
# GRN PRODUCT × STORE × DAY
# ------------------------------------------------------------------------------

create_or_replace_view(
    f"{BASE}.vw_grn_product_daily",
    f"""
    SELECT

        store_key,
        store_id,
        store_name,

        product_key,
        barcode,
        product_name,

        brand_name,
        category_name,

        date_key,
        grn_date,

        SUM(received_quantity)
            AS received_quantity,

        SUM(total_cost)
            AS purchase_amount,

        SUM(amount_net)
            AS amount_net

    FROM {BASE}.vw_grn

    GROUP BY

        store_key,
        store_id,
        store_name,

        product_key,
        barcode,
        product_name,

        brand_name,
        category_name,

        date_key,
        grn_date
    """
)


print("✅ GRN semantic views created")


# ================================================================================
# 10. VIEW INVENTORY
# ================================================================================

print("\n[9/12] Validating created views...")

semantic_views = [

    # Dimensions
    "vw_dim_store",
    "vw_dim_product",
    "vw_dim_date",

    # Sales
    "vw_sales",
    "vw_sales_store_daily",
    "vw_sales_product_daily",
    "vw_sales_brand_daily",
    "vw_sales_category_daily",

    # Stock
    "vw_stock",
    "vw_stock_latest",
    "vw_stock_store_daily",

    # RTV
    "vw_rtv",
    "vw_rtv_store_daily",
    "vw_rtv_product_daily",

    # GRN
    "vw_grn",
    "vw_grn_store_daily",
    "vw_grn_product_daily",
]


for view_name in semantic_views:

    full_name = f"{BASE}.{view_name}"

    if not spark.catalog.tableExists(full_name):

        raise RuntimeError(
            f"❌ Expected semantic view was not created: "
            f"{full_name}"
        )

    print(f"✅ {full_name}")


# ================================================================================
# 11. BASIC ROW-COUNT VALIDATION
# ================================================================================

print("\n[10/12] Running basic semantic-view validation...")


def get_count(view_name):

    return spark.sql(
        f"""
        SELECT COUNT(*) AS cnt
        FROM {view_name}
        """
    ).first()["cnt"]


sales_count = get_count(
    f"{BASE}.vw_sales"
)

stock_count = get_count(
    f"{BASE}.vw_stock"
)

rtv_count = get_count(
    f"{BASE}.vw_rtv"
)

grn_count = get_count(
    f"{BASE}.vw_grn"
)


print(f"Sales semantic rows : {sales_count:,}")
print(f"Stock semantic rows : {stock_count:,}")
print(f"RTV semantic rows   : {rtv_count:,}")
print(f"GRN semantic rows   : {grn_count:,}")


if sales_count == 0:

    raise RuntimeError(
        "❌ vw_sales contains zero rows."
    )


if stock_count == 0:

    print(
        "⚠️ WARNING: vw_stock contains zero rows."
    )


if rtv_count == 0:

    print(
        "⚠️ WARNING: vw_rtv contains zero rows."
    )


if grn_count == 0:

    print(
        "⚠️ WARNING: vw_grn contains zero rows."
    )


# ================================================================================
# 12. KEY / BUSINESS / RECONCILIATION VALIDATION
# ================================================================================

print("\n[11/12] Running semantic integrity validation...")


# ================================================================================
# 12A. NULL SURROGATE-KEY VALIDATION
# ================================================================================

key_checks = {

    "sales": (
        f"{BASE}.vw_sales",
        [
            "store_key",
            "product_key",
            "date_key",
        ],
    ),

    "stock": (
        f"{BASE}.vw_stock",
        [
            "store_key",
            "product_key",
            "date_key",
        ],
    ),

    "rtv": (
        f"{BASE}.vw_rtv",
        [
            "store_key",
            "product_key",
            "date_key",
        ],
    ),

    "grn": (
        f"{BASE}.vw_grn",
        [
            "store_key",
            "product_key",
            "date_key",
        ],
    ),
}


for name, (view_name, columns) in key_checks.items():

    expressions = [

        f"""
        SUM(
            CASE
                WHEN {column} IS NULL
                THEN 1
                ELSE 0
            END
        ) AS {column}_nulls
        """

        for column in columns
    ]


    result = spark.sql(
        f"""
        SELECT
            {", ".join(expressions)}
        FROM {view_name}
        """
    ).first()


    for column in columns:

        null_count = result[
            f"{column}_nulls"
        ]


        if null_count != 0:

            raise RuntimeError(
                f"""
❌ {view_name}: {column} contains
{null_count:,} NULL keys.
"""
            )


        print(
            f"✅ {name}: "
            f"{column} NULL validation passed"
        )


# ================================================================================
# 12B. HO MARLBORO VALIDATION
# ================================================================================

ho_marlboro_count = spark.sql(
    f"""
    SELECT COUNT(*) AS cnt

    FROM {BASE}.vw_sales

    WHERE LOWER(
        TRIM(
            COALESCE(
                product_name,
                ''
            )
        )
    ) = 'ho marlboro'
    """
).first()["cnt"]


if ho_marlboro_count != 0:

    raise RuntimeError(
        f"""
❌ Ho Marlboro exclusion failed.

Rows remaining:
{ho_marlboro_count:,}
"""
    )


print(
    "✅ Ho Marlboro exclusion validated"
)


# ================================================================================
# 12C. SALES ROW RECONCILIATION
#
# Recreate the same joins used by vw_sales directly against fact_sales.
# This is necessary because fact_sales itself does not contain surrogate keys.
# ================================================================================

print(
    "\nValidating vw_sales against fact_sales..."
)


fact_sales_after_business_rules = spark.sql(
    f"""
    WITH sales_normalized AS (

        SELECT

            fs.*,

            {normalize_store_name_sql("fs.storeName")}
                AS normalized_store_name,

            CAST(fs.barcode AS STRING)
                AS normalized_barcode,

            CAST(fs.orderDate AS DATE)
                AS order_date

        FROM {FACT_SALES} fs
    )

    SELECT COUNT(*) AS cnt

    FROM sales_normalized s

    INNER JOIN {DIM_STORE} ds

        ON s.normalized_store_name
           =
           ds.normalized_store_name

    INNER JOIN {DIM_PRODUCT} dp

        ON s.normalized_barcode
           =
           CAST(dp.barcode AS STRING)

    INNER JOIN {DIM_DATE} dd

        ON s.order_date
           =
           dd.date

    WHERE LOWER(
        TRIM(
            COALESCE(
                dp.product_name,
                ''
            )
        )
    ) <> 'ho marlboro'
    """
).first()["cnt"]


semantic_sales_rows = spark.sql(
    f"""
    SELECT COUNT(*) AS cnt
    FROM {BASE}.vw_sales
    """
).first()["cnt"]


print(
    f"Fact sales rows after semantic joins "
    f"and Ho Marlboro exclusion: "
    f"{fact_sales_after_business_rules:,}"
)

print(
    f"Semantic sales rows: "
    f"{semantic_sales_rows:,}"
)


if (
    fact_sales_after_business_rules
    != semantic_sales_rows
):

    raise RuntimeError(
        f"""
❌ Sales semantic row reconciliation failed.

Fact-side result :
{fact_sales_after_business_rules:,}

Semantic view    :
{semantic_sales_rows:,}

Difference       :
{abs(
    fact_sales_after_business_rules
    - semantic_sales_rows
):,}
"""
    )


print(
    "✅ Sales row reconciliation passed"
)


# ================================================================================
# 12D. SALES AMOUNT RECONCILIATION
# ================================================================================

fact_sales_amount = spark.sql(
    f"""
    WITH sales_normalized AS (

        SELECT

            fs.*,

            {normalize_store_name_sql("fs.storeName")}
                AS normalized_store_name,

            CAST(fs.barcode AS STRING)
                AS normalized_barcode,

            CAST(fs.orderDate AS DATE)
                AS order_date

        FROM {FACT_SALES} fs
    )

    SELECT

        COALESCE(
            SUM(s.totalProductPrice),
            0.0
        ) AS amount

    FROM sales_normalized s

    INNER JOIN {DIM_STORE} ds

        ON s.normalized_store_name
           =
           ds.normalized_store_name

    INNER JOIN {DIM_PRODUCT} dp

        ON s.normalized_barcode
           =
           CAST(dp.barcode AS STRING)

    INNER JOIN {DIM_DATE} dd

        ON s.order_date
           =
           dd.date

    WHERE LOWER(
        TRIM(
            COALESCE(
                dp.product_name,
                ''
            )
        )
    ) <> 'ho marlboro'
    """
).first()["amount"]


semantic_sales_amount = spark.sql(
    f"""
    SELECT

        COALESCE(
            SUM(total_product_price),
            0.0
        ) AS amount

    FROM {BASE}.vw_sales
    """
).first()["amount"]


sales_amount_difference = abs(
    float(fact_sales_amount)
    -
    float(semantic_sales_amount)
)


print(
    f"Fact-side sales amount : "
    f"₹{float(fact_sales_amount):,.2f}"
)

print(
    f"Semantic sales amount  : "
    f"₹{float(semantic_sales_amount):,.2f}"
)

print(
    f"Difference             : "
    f"₹{sales_amount_difference:,.2f}"
)


if sales_amount_difference > 0.01:

    raise RuntimeError(
        f"""
❌ Sales amount reconciliation failed.

Fact amount:
₹{float(fact_sales_amount):,.2f}

Semantic amount:
₹{float(semantic_sales_amount):,.2f}

Difference:
₹{sales_amount_difference:,.2f}
"""
    )


print(
    "✅ Sales amount reconciliation passed"
)


# ================================================================================
# 12E. SALES QUANTITY RECONCILIATION
# ================================================================================

fact_sales_quantity = spark.sql(
    f"""
    WITH sales_normalized AS (

        SELECT

            fs.*,

            {normalize_store_name_sql("fs.storeName")}
                AS normalized_store_name,

            CAST(fs.barcode AS STRING)
                AS normalized_barcode,

            CAST(fs.orderDate AS DATE)
                AS order_date

        FROM {FACT_SALES} fs
    )

    SELECT

        COALESCE(
            SUM(s.quantity),
            0
        ) AS quantity

    FROM sales_normalized s

    INNER JOIN {DIM_STORE} ds

        ON s.normalized_store_name
           =
           ds.normalized_store_name

    INNER JOIN {DIM_PRODUCT} dp

        ON s.normalized_barcode
           =
           CAST(dp.barcode AS STRING)

    INNER JOIN {DIM_DATE} dd

        ON s.order_date
           =
           dd.date

    WHERE LOWER(
        TRIM(
            COALESCE(
                dp.product_name,
                ''
            )
        )
    ) <> 'ho marlboro'
    """
).first()["quantity"]


semantic_sales_quantity = spark.sql(
    f"""
    SELECT

        COALESCE(
            SUM(quantity),
            0
        ) AS quantity

    FROM {BASE}.vw_sales
    """
).first()["quantity"]


if (
    float(fact_sales_quantity)
    !=
    float(semantic_sales_quantity)
):

    raise RuntimeError(
        f"""
❌ Sales quantity reconciliation failed.

Fact quantity:
{fact_sales_quantity}

Semantic quantity:
{semantic_sales_quantity}
"""
    )


print(
    f"✅ Sales quantity reconciliation passed: "
    f"{semantic_sales_quantity}"
)


# ================================================================================
# 12F. FACT / SEMANTIC ROW COUNTS FOR OTHER SUBJECT AREAS
# ================================================================================

print(
    "\nValidating other semantic views..."
)


# ------------------------------------------------------------------------------
# STOCK
# ------------------------------------------------------------------------------

fact_stock_count = spark.sql(
    f"""
    SELECT COUNT(*) AS cnt
    FROM {FACT_STOCK}
    """
).first()["cnt"]


semantic_stock_count = spark.sql(
    f"""
    SELECT COUNT(*) AS cnt
    FROM {BASE}.vw_stock
    """
).first()["cnt"]


if fact_stock_count != semantic_stock_count:

    raise RuntimeError(
        f"""
❌ Stock semantic row reconciliation failed.

Fact:
{fact_stock_count:,}

Semantic:
{semantic_stock_count:,}
"""
    )


print(
    f"✅ Stock row reconciliation passed: "
    f"{semantic_stock_count:,}"
)


# ------------------------------------------------------------------------------
# RTV
# ------------------------------------------------------------------------------

fact_rtv_count = spark.sql(
    f"""
    SELECT COUNT(*) AS cnt
    FROM {FACT_RTV}
    """
).first()["cnt"]


semantic_rtv_count = spark.sql(
    f"""
    SELECT COUNT(*) AS cnt
    FROM {BASE}.vw_rtv
    """
).first()["cnt"]


if fact_rtv_count != semantic_rtv_count:

    raise RuntimeError(
        f"""
❌ RTV semantic row reconciliation failed.

Fact:
{fact_rtv_count:,}

Semantic:
{semantic_rtv_count:,}
"""
    )


print(
    f"✅ RTV row reconciliation passed: "
    f"{semantic_rtv_count:,}"
)


# ------------------------------------------------------------------------------
# GRN
# ------------------------------------------------------------------------------

fact_grn_count = spark.sql(
    f"""
    SELECT COUNT(*) AS cnt
    FROM {FACT_GRN}
    """
).first()["cnt"]


semantic_grn_count = spark.sql(
    f"""
    SELECT COUNT(*) AS cnt
    FROM {BASE}.vw_grn
    """
).first()["cnt"]


if fact_grn_count != semantic_grn_count:

    raise RuntimeError(
        f"""
❌ GRN semantic row reconciliation failed.

Fact:
{fact_grn_count:,}

Semantic:
{semantic_grn_count:,}
"""
    )


print(
    f"✅ GRN row reconciliation passed: "
    f"{semantic_grn_count:,}"
)


# ================================================================================
# 13. FINAL SUMMARY
# ================================================================================

print("\n[12/12] Final semantic-layer summary")


print("\n")
print("=" * 80)
print("25_SEMANTIC_VIEWS COMPLETED SUCCESSFULLY")
print("=" * 80)


print(
    """
DIMENSION VIEWS
---------------
vw_dim_store
vw_dim_product
vw_dim_date


SALES VIEWS
-----------
vw_sales
vw_sales_store_daily
vw_sales_product_daily
vw_sales_brand_daily
vw_sales_category_daily


STOCK VIEWS
-----------
vw_stock
vw_stock_latest
vw_stock_store_daily


RTV VIEWS
---------
vw_rtv
vw_rtv_store_daily
vw_rtv_product_daily


GRN VIEWS
---------
vw_grn
vw_grn_store_daily
vw_grn_product_daily
"""
)


print("=" * 80)
print("VALIDATION STATUS")
print("=" * 80)

print("✅ Required tables validated")
print("✅ Actual fact_sales schema handled")
print("✅ Store normalization applied")
print("✅ Barcode-based product matching applied")
print("✅ Date dimension matching applied")
print("✅ Dimension views created")
print("✅ Sales semantic views created")
print("✅ Stock semantic views created")
print("✅ RTV semantic views created")
print("✅ GRN semantic views created")
print("✅ NULL surrogate-key validation passed")
print("✅ Ho Marlboro exclusion validated")
print("✅ Sales row reconciliation passed")
print("✅ Sales amount reconciliation passed")
print("✅ Sales quantity reconciliation passed")
print("✅ Stock reconciliation passed")
print("✅ RTV reconciliation passed")
print("✅ GRN reconciliation passed")
print("✅ Semantic layer ready for downstream validation")

print("=" * 80)

# COMMAND ----------

