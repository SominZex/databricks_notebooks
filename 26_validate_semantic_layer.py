# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# Databricks notebook source

# ================================================================================
# 26_VALIDATE_SEMANTIC_LAYER
# ================================================================================
#
# Purpose:
#   Validate the new Databricks semantic layer against the existing production
#   Gold tables.
#
# IMPORTANT:
#   This notebook is READ-ONLY.
#
#   It does NOT:
#       - modify fact tables
#       - modify dimension tables
#       - modify Gold tables
#       - modify semantic views
#
#   It only compares:
#
#       EXISTING PRODUCTION GOLD
#                    VS
#       NEW DATABRICKS SEMANTIC LAYER
#
# Business rules:
#   1. Sales reporting excludes Ho Marlboro
#   2. Sales reporting does NOT filter orderStatus = 'PLACED'
#   3. Store matching is based on normalized store identity
#   4. Product matching is barcode based
#   5. Semantic sales amount = SUM(total_product_price)
#
# ================================================================================


from pyspark.sql import functions as F


# ================================================================================
# 0. CONFIGURATION
# ================================================================================

CATALOG = "billing"
SCHEMA = "sales_data"

BASE = f"{CATALOG}.{SCHEMA}"


# ------------------------------------------------------------------------------
# Production Gold tables
# ------------------------------------------------------------------------------

GOLD_DAILY = f"{BASE}.gold_daily_sales"
GOLD_STORE = f"{BASE}.gold_store_performance"
GOLD_BRAND = f"{BASE}.gold_brand_performance"
GOLD_PRODUCT = f"{BASE}.gold_product_performance"
GOLD_CATEGORY = f"{BASE}.gold_category_performance"

GOLD_STOCK = f"{BASE}.gold_stock"
GOLD_RTV = f"{BASE}.gold_rtv"
GOLD_GRN = f"{BASE}.gold_grn"


# ------------------------------------------------------------------------------
# Semantic views
# ------------------------------------------------------------------------------

VW_SALES = f"{BASE}.vw_sales"
VW_SALES_STORE = f"{BASE}.vw_sales_store_daily"
VW_SALES_BRAND = f"{BASE}.vw_sales_brand_daily"
VW_SALES_PRODUCT = f"{BASE}.vw_sales_product_daily"
VW_SALES_CATEGORY = f"{BASE}.vw_sales_category_daily"

VW_STOCK = f"{BASE}.vw_stock"
VW_STOCK_LATEST = f"{BASE}.vw_stock_latest"
VW_STOCK_STORE = f"{BASE}.vw_stock_store_daily"

VW_RTV = f"{BASE}.vw_rtv"
VW_RTV_STORE = f"{BASE}.vw_rtv_store_daily"
VW_RTV_PRODUCT = f"{BASE}.vw_rtv_product_daily"

VW_GRN = f"{BASE}.vw_grn"
VW_GRN_STORE = f"{BASE}.vw_grn_store_daily"
VW_GRN_PRODUCT = f"{BASE}.vw_grn_product_daily"


# ------------------------------------------------------------------------------
# Tolerances
# ------------------------------------------------------------------------------

AMOUNT_TOLERANCE = 0.01
QUANTITY_TOLERANCE = 0
COUNT_TOLERANCE = 0
AOV_TOLERANCE = 0.01

# Percentage difference allowed for diagnostic classification.
# We do NOT automatically fail purely because of percentage difference.
PERCENT_TOLERANCE = 0.01


print("=" * 80)
print("26 VALIDATE SEMANTIC LAYER")
print("=" * 80)


# ================================================================================
# 1. REQUIRED OBJECT VALIDATION
# ================================================================================

print("\n[1/12] Validating required production Gold tables and semantic views...")


required_objects = [

    # Production Gold
    GOLD_DAILY,
    GOLD_STORE,
    GOLD_BRAND,
    GOLD_PRODUCT,
    GOLD_CATEGORY,
    GOLD_STOCK,
    GOLD_RTV,
    GOLD_GRN,

    # Semantic
    VW_SALES,
    VW_SALES_STORE,
    VW_SALES_BRAND,
    VW_SALES_PRODUCT,
    VW_SALES_CATEGORY,

    VW_STOCK,
    VW_STOCK_LATEST,
    VW_STOCK_STORE,

    VW_RTV,
    VW_RTV_STORE,
    VW_RTV_PRODUCT,

    VW_GRN,
    VW_GRN_STORE,
    VW_GRN_PRODUCT,
]


missing_objects = []

for object_name in required_objects:

    if not spark.catalog.tableExists(object_name):

        missing_objects.append(object_name)

        print(f"❌ Missing: {object_name}")

    else:

        print(f"✅ {object_name}")


if missing_objects:

    raise RuntimeError(
        "\n❌ Required validation objects are missing:\n"
        + "\n".join(missing_objects)
    )


print("\n✅ All required validation objects exist")


# ================================================================================
# 2. HELPER FUNCTIONS
# ================================================================================

print("\n[2/12] Initializing validation helpers...")


def compare_values(
    production_value,
    semantic_value,
    tolerance=0.01
):

    if production_value is None:
        production_value = 0

    if semantic_value is None:
        semantic_value = 0

    difference = abs(
        float(production_value)
        - float(semantic_value)
    )

    return difference <= tolerance


def percentage_difference(
    production_value,
    semantic_value
):

    if production_value is None:
        production_value = 0

    if semantic_value is None:
        semantic_value = 0

    production_value = float(production_value)
    semantic_value = float(semantic_value)

    denominator = abs(production_value)

    if denominator == 0:

        if semantic_value == 0:
            return 0.0

        return float("inf")

    return (
        abs(production_value - semantic_value)
        / denominator
    ) * 100.0


def print_comparison(
    metric,
    production_value,
    semantic_value,
    tolerance=0.01
):

    if production_value is None:
        production_value = 0

    if semantic_value is None:
        semantic_value = 0

    difference = abs(
        float(production_value)
        - float(semantic_value)
    )

    pct = percentage_difference(
        production_value,
        semantic_value
    )

    passed = difference <= tolerance

    status = "✅ PASS" if passed else "❌ DIFFERENCE"

    print(
        f"{status} | "
        f"{metric:<30} | "
        f"Production: {production_value} | "
        f"Semantic: {semantic_value} | "
        f"Diff: {difference}"
    )

    return passed


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
# 3. BASIC SEMANTIC COUNTS
# ================================================================================

print("\n[3/12] Reading semantic-layer baseline...")


baseline = spark.sql(
    f"""
    SELECT

        (SELECT COUNT(*) FROM {VW_SALES})
            AS sales_rows,

        (SELECT COUNT(*) FROM {VW_STOCK})
            AS stock_rows,

        (SELECT COUNT(*) FROM {VW_RTV})
            AS rtv_rows,

        (SELECT COUNT(*) FROM {VW_GRN})
            AS grn_rows
    """
).first()


print(
    f"Semantic sales rows : {baseline['sales_rows']:,}"
)

print(
    f"Semantic stock rows : {baseline['stock_rows']:,}"
)

print(
    f"Semantic RTV rows   : {baseline['rtv_rows']:,}"
)

print(
    f"Semantic GRN rows   : {baseline['grn_rows']:,}"
)


if baseline["sales_rows"] == 0:

    raise RuntimeError(
        "❌ Semantic sales view contains zero rows."
    )


# ================================================================================
# 4. GLOBAL SALES RECONCILIATION
# ================================================================================

print("\n[4/12] Validating global sales metrics...")


production_sales = spark.sql(
    f"""
    SELECT

        COUNT(*) AS row_count,

        COALESCE(
            SUM(sales),
            0.0
        ) AS sales_amount,

        COALESCE(
            SUM(nooforders),
            0
        ) AS order_count,

        COALESCE(
            SUM(quantitysold),
            0
        ) AS quantity_sold

    FROM {GOLD_PRODUCT}
    """
).first()


semantic_sales = spark.sql(
    f"""
    SELECT

        COUNT(*) AS row_count,

        COALESCE(
            SUM(total_product_price),
            0.0
        ) AS sales_amount,

        COUNT(DISTINCT invoice) AS order_count,

        COALESCE(
            SUM(quantity),
            0
        ) AS quantity_sold

    FROM {VW_SALES}
    """
).first()


print("\nGLOBAL SALES")

print_comparison(
    "Sales amount",
    production_sales["sales_amount"],
    semantic_sales["sales_amount"],
    AMOUNT_TOLERANCE
)

print_comparison(
    "Order count",
    production_sales["order_count"],
    semantic_sales["order_count"],
    COUNT_TOLERANCE
)

print_comparison(
    "Quantity sold",
    production_sales["quantity_sold"],
    semantic_sales["quantity_sold"],
    QUANTITY_TOLERANCE
)


# ================================================================================
# 5. STORE PERFORMANCE RECONCILIATION
# ================================================================================

print("\n[5/12] Validating store performance...")


production_store = spark.sql(
    f"""
    SELECT

        orderDate AS date,

        {normalize_store_name_sql("storeName")}
            AS normalized_store_name,

        storeName,

        SUM(sales) AS sales_amount,

        SUM(nooforder) AS order_count,

        AVG(aov) AS production_aov

    FROM {GOLD_STORE}

    GROUP BY

        orderDate,
        storeName
    """
)


semantic_store = spark.sql(
    f"""
    SELECT

        date,

        {normalize_store_name_sql("store_name")}
            AS normalized_store_name,

        store_name,

        SUM(sales_amount) AS sales_amount,

        SUM(invoice_count) AS order_count,

        CASE
            WHEN SUM(invoice_count) = 0
                THEN 0.0
            ELSE
                SUM(sales_amount)
                /
                SUM(invoice_count)
        END AS semantic_aov

    FROM {VW_SALES_STORE}

    GROUP BY

        date,
        store_name
    """
)


store_comparison = (

    production_store.alias("p")

    .join(
        semantic_store.alias("s"),

        (
            (F.col("p.date") == F.col("s.date"))
            &
            (
                F.col("p.normalized_store_name")
                ==
                F.col("s.normalized_store_name")
            )
        ),

        "full"
    )

    .select(

        F.coalesce(
            F.col("p.date"),
            F.col("s.date")
        ).alias("date"),

        F.coalesce(
            F.col("p.storeName"),
            F.col("s.store_name")
        ).alias("store_name"),

        F.coalesce(
            F.col("p.sales_amount"),
            F.lit(0.0)
        ).alias("production_sales"),

        F.coalesce(
            F.col("s.sales_amount"),
            F.lit(0.0)
        ).alias("semantic_sales"),

        F.coalesce(
            F.col("p.order_count"),
            F.lit(0)
        ).alias("production_orders"),

        F.coalesce(
            F.col("s.order_count"),
            F.lit(0)
        ).alias("semantic_orders"),

        F.coalesce(
            F.col("p.production_aov"),
            F.lit(0.0)
        ).alias("production_aov"),

        F.coalesce(
            F.col("s.semantic_aov"),
            F.lit(0.0)
        ).alias("semantic_aov")
    )
)


store_comparison = (

    store_comparison

    .withColumn(
        "sales_difference",
        F.abs(
            F.col("production_sales")
            -
            F.col("semantic_sales")
        )
    )

    .withColumn(
        "order_difference",
        F.abs(
            F.col("production_orders")
            -
            F.col("semantic_orders")
        )
    )

    .withColumn(
        "aov_difference",
        F.abs(
            F.col("production_aov")
            -
            F.col("semantic_aov")
        )
    )

    .withColumn(
        "sales_status",

        F.when(
            F.col("sales_difference")
            <= F.lit(AMOUNT_TOLERANCE),

            "PASS"

        ).otherwise("DIFFERENCE")
    )

    .withColumn(
        "order_status",

        F.when(
            F.col("order_difference")
            <= F.lit(COUNT_TOLERANCE),

            "PASS"

        ).otherwise("DIFFERENCE")
    )

    .withColumn(
        "aov_status",

        F.when(
            F.col("aov_difference")
            <= F.lit(AOV_TOLERANCE),

            "PASS"

        ).otherwise("DIFFERENCE")
    )
)


store_sales_differences = store_comparison.filter(
    F.col("sales_status") != "PASS"
)

store_order_differences = store_comparison.filter(
    F.col("order_status") != "PASS"
)

store_aov_differences = store_comparison.filter(
    F.col("aov_status") != "PASS"
)


print(
    f"Store/date combinations checked : "
    f"{store_comparison.count():,}"
)

print(
    f"Sales differences               : "
    f"{store_sales_differences.count():,}"
)

print(
    f"Order differences               : "
    f"{store_order_differences.count():,}"
)

print(
    f"AOV differences                 : "
    f"{store_aov_differences.count():,}"
)


if store_sales_differences.count() == 0:

    print("✅ Store sales reconciliation passed")

else:

    print(
        "⚠️ Store sales differences detected."
    )

    store_sales_differences.orderBy(
        F.desc("sales_difference")
    ).show(20, truncate=False)


if store_order_differences.count() == 0:

    print("✅ Store order reconciliation passed")

else:

    print(
        "⚠️ Store order differences detected."
    )

    store_order_differences.orderBy(
        F.desc("order_difference")
    ).show(20, truncate=False)


# AOV is diagnostic because production AOV is stored independently
# while semantic AOV is recalculated from sales / invoices.

if store_aov_differences.count() == 0:

    print("✅ Store AOV reconciliation passed")

else:

    print(
        "⚠️ Store AOV differences detected."
    )

    store_aov_differences.orderBy(
        F.desc("aov_difference")
    ).show(20, truncate=False)


# ================================================================================
# 6. BRAND PERFORMANCE RECONCILIATION
# ================================================================================

print("\n[6/12] Validating brand performance...")


production_brand = spark.sql(
    f"""
    SELECT

        orderDate AS date,

        LOWER(TRIM(brandName))
            AS brand_name,

        SUM(sales) AS sales_amount,

        SUM(nooforders) AS order_count

    FROM {GOLD_BRAND}

    GROUP BY

        orderDate,
        LOWER(TRIM(brandName))
    """
)


semantic_brand = spark.sql(
    f"""
    SELECT

        date,

        LOWER(TRIM(brand_name))
            AS brand_name,

        SUM(sales_amount) AS sales_amount,

        SUM(invoice_count) AS order_count

    FROM {VW_SALES_BRAND}

    GROUP BY

        date,
        LOWER(TRIM(brand_name))
    """
)


brand_comparison = (

    production_brand.alias("p")

    .join(
        semantic_brand.alias("s"),

        (
            (F.col("p.date") == F.col("s.date"))
            &
            (
                F.col("p.brand_name")
                ==
                F.col("s.brand_name")
            )
        ),

        "full"
    )

    .select(

        F.coalesce(
            F.col("p.date"),
            F.col("s.date")
        ).alias("date"),

        F.coalesce(
            F.col("p.brand_name"),
            F.col("s.brand_name")
        ).alias("brand_name"),

        F.coalesce(
            F.col("p.sales_amount"),
            F.lit(0.0)
        ).alias("production_sales"),

        F.coalesce(
            F.col("s.sales_amount"),
            F.lit(0.0)
        ).alias("semantic_sales"),

        F.coalesce(
            F.col("p.order_count"),
            F.lit(0)
        ).alias("production_orders"),

        F.coalesce(
            F.col("s.order_count"),
            F.lit(0)
        ).alias("semantic_orders")
    )

    .withColumn(
        "sales_difference",
        F.abs(
            F.col("production_sales")
            -
            F.col("semantic_sales")
        )
    )

    .withColumn(
        "order_difference",
        F.abs(
            F.col("production_orders")
            -
            F.col("semantic_orders")
        )
    )
)


brand_sales_diff = brand_comparison.filter(
    F.col("sales_difference")
    > F.lit(AMOUNT_TOLERANCE)
)

brand_order_diff = brand_comparison.filter(
    F.col("order_difference")
    > F.lit(COUNT_TOLERANCE)
)


print(
    f"Brand/date combinations checked : "
    f"{brand_comparison.count():,}"
)

print(
    f"Brand sales differences         : "
    f"{brand_sales_diff.count():,}"
)

print(
    f"Brand order differences         : "
    f"{brand_order_diff.count():,}"
)


if brand_sales_diff.count() == 0:

    print("✅ Brand sales reconciliation passed")

else:

    print("⚠️ Brand sales differences detected")

    brand_sales_diff.orderBy(
        F.desc("sales_difference")
    ).show(20, truncate=False)


if brand_order_diff.count() == 0:

    print("✅ Brand order reconciliation passed")

else:

    print("⚠️ Brand order differences detected")

    brand_order_diff.orderBy(
        F.desc("order_difference")
    ).show(20, truncate=False)


# ================================================================================
# 7. PRODUCT PERFORMANCE RECONCILIATION
# ================================================================================

print("\n[7/12] Validating product performance...")


production_product = spark.sql(
    f"""
    SELECT

        orderDate AS date,

        LOWER(TRIM(productName))
            AS product_name,

        SUM(sales) AS sales_amount,

        SUM(nooforders) AS order_count,

        SUM(quantitysold) AS quantity_sold

    FROM {GOLD_PRODUCT}

    GROUP BY

        orderDate,
        LOWER(TRIM(productName))
    """
)


semantic_product = spark.sql(
    f"""
    SELECT

        date,

        LOWER(TRIM(product_name))
            AS product_name,

        SUM(sales_amount) AS sales_amount,

        SUM(invoice_count) AS order_count,

        SUM(quantity_sold) AS quantity_sold

    FROM {VW_SALES_PRODUCT}

    GROUP BY

        date,
        LOWER(TRIM(product_name))
    """
)


product_comparison = (

    production_product.alias("p")

    .join(
        semantic_product.alias("s"),

        (
            (F.col("p.date") == F.col("s.date"))
            &
            (
                F.col("p.product_name")
                ==
                F.col("s.product_name")
            )
        ),

        "full"
    )

    .select(

        F.coalesce(
            F.col("p.date"),
            F.col("s.date")
        ).alias("date"),

        F.coalesce(
            F.col("p.product_name"),
            F.col("s.product_name")
        ).alias("product_name"),

        F.coalesce(
            F.col("p.sales_amount"),
            F.lit(0.0)
        ).alias("production_sales"),

        F.coalesce(
            F.col("s.sales_amount"),
            F.lit(0.0)
        ).alias("semantic_sales"),

        F.coalesce(
            F.col("p.order_count"),
            F.lit(0)
        ).alias("production_orders"),

        F.coalesce(
            F.col("s.order_count"),
            F.lit(0)
        ).alias("semantic_orders"),

        F.coalesce(
            F.col("p.quantity_sold"),
            F.lit(0)
        ).alias("production_quantity"),

        F.coalesce(
            F.col("s.quantity_sold"),
            F.lit(0)
        ).alias("semantic_quantity")
    )

    .withColumn(
        "sales_difference",
        F.abs(
            F.col("production_sales")
            -
            F.col("semantic_sales")
        )
    )

    .withColumn(
        "order_difference",
        F.abs(
            F.col("production_orders")
            -
            F.col("semantic_orders")
        )
    )

    .withColumn(
        "quantity_difference",
        F.abs(
            F.col("production_quantity")
            -
            F.col("semantic_quantity")
        )
    )
)


product_sales_diff = product_comparison.filter(
    F.col("sales_difference")
    > F.lit(AMOUNT_TOLERANCE)
)

product_order_diff = product_comparison.filter(
    F.col("order_difference")
    > F.lit(COUNT_TOLERANCE)
)

product_quantity_diff = product_comparison.filter(
    F.col("quantity_difference")
    > F.lit(QUANTITY_TOLERANCE)
)


print(
    f"Product/date combinations checked : "
    f"{product_comparison.count():,}"
)

print(
    f"Product sales differences         : "
    f"{product_sales_diff.count():,}"
)

print(
    f"Product order differences         : "
    f"{product_order_diff.count():,}"
)

print(
    f"Product quantity differences      : "
    f"{product_quantity_diff.count():,}"
)


if product_sales_diff.count() == 0:

    print("✅ Product sales reconciliation passed")

else:

    print("⚠️ Product sales differences detected")

    product_sales_diff.orderBy(
        F.desc("sales_difference")
    ).show(20, truncate=False)


if product_order_diff.count() == 0:

    print("✅ Product order reconciliation passed")

else:

    print("⚠️ Product order differences detected")

    product_order_diff.orderBy(
        F.desc("order_difference")
    ).show(20, truncate=False)


if product_quantity_diff.count() == 0:

    print("✅ Product quantity reconciliation passed")

else:

    print("⚠️ Product quantity differences detected")

    product_quantity_diff.orderBy(
        F.desc("quantity_difference")
    ).show(20, truncate=False)


# ================================================================================
# 8. CATEGORY VALIDATION
# ================================================================================

print("\n[8/12] Validating category definitions...")


print(
    """
IMPORTANT CATEGORY MODEL DIFFERENCE
-----------------------------------

Production Gold:
    gold_category_performance.subCategoryOf

Semantic:
    vw_sales_category_daily.category_name

These are NOT assumed to be equivalent.

Therefore this notebook performs a diagnostic comparison only.
It does NOT declare the category migration valid unless the definitions
are confirmed equivalent.
"""
)


production_category = spark.sql(
    f"""
    SELECT

        orderDate AS date,

        LOWER(TRIM(subCategoryOf))
            AS category_name,

        SUM(sales) AS sales_amount

    FROM {GOLD_CATEGORY}

    GROUP BY

        orderDate,
        LOWER(TRIM(subCategoryOf))
    """
)


semantic_category = spark.sql(
    f"""
    SELECT

        date,

        LOWER(TRIM(category_name))
            AS category_name,

        SUM(sales_amount) AS sales_amount

    FROM {VW_SALES_CATEGORY}

    GROUP BY

        date,
        LOWER(TRIM(category_name))
    """
)


category_comparison = (

    production_category.alias("p")

    .join(
        semantic_category.alias("s"),

        (
            (F.col("p.date") == F.col("s.date"))
            &
            (
                F.col("p.category_name")
                ==
                F.col("s.category_name")
            )
        ),

        "full"
    )

    .select(

        F.coalesce(
            F.col("p.date"),
            F.col("s.date")
        ).alias("date"),

        F.coalesce(
            F.col("p.category_name"),
            F.col("s.category_name")
        ).alias("category_name"),

        F.coalesce(
            F.col("p.sales_amount"),
            F.lit(0.0)
        ).alias("production_sales"),

        F.coalesce(
            F.col("s.sales_amount"),
            F.lit(0.0)
        ).alias("semantic_sales")
    )

    .withColumn(
        "sales_difference",
        F.abs(
            F.col("production_sales")
            -
            F.col("semantic_sales")
        )
    )
)


category_differences = category_comparison.filter(
    F.col("sales_difference")
    > F.lit(AMOUNT_TOLERANCE)
)


print(
    f"Category/date combinations checked : "
    f"{category_comparison.count():,}"
)

print(
    f"Category differences               : "
    f"{category_differences.count():,}"
)


if category_differences.count() == 0:

    print(
        "ℹ️ Category values currently reconcile."
    )

else:

    print(
        "⚠️ Category differences detected."
    )

    category_differences.orderBy(
        F.desc("sales_difference")
    ).show(30, truncate=False)


# ================================================================================
# 9. DAILY SALES VALIDATION
# ================================================================================

print("\n[9/12] Validating daily sales...")


print(
    """
Production gold_daily_sales contains:

    total_net_sales
    total_orders
    total_discounts
    tax
    delivery fees
    payment-method totals

The semantic sales view uses:

    SUM(totalProductPrice)

Therefore total_net_sales is treated as a separate production metric.

We first compare daily orders and sales diagnostically.
"""
)


production_daily = spark.sql(
    f"""
    SELECT

        orderDate AS date,

        SUM(total_orders) AS order_count,

        SUM(total_net_sales) AS net_sales,

        SUM(total_discounts) AS discounts,

        SUM(total_delivery_fees) AS delivery_fees

    FROM {GOLD_DAILY}

    GROUP BY orderDate
    """
)


semantic_daily = spark.sql(
    f"""
    SELECT

        date,

        SUM(invoice_count) AS order_count,

        SUM(sales_amount) AS sales_amount,

        SUM(discount_amount) AS discounts

    FROM {VW_SALES_STORE}

    GROUP BY date
    """
)


daily_comparison = (

    production_daily.alias("p")

    .join(
        semantic_daily.alias("s"),

        F.col("p.date") == F.col("s.date"),

        "full"
    )

    .select(

        F.coalesce(
            F.col("p.date"),
            F.col("s.date")
        ).alias("date"),

        F.coalesce(
            F.col("p.order_count"),
            F.lit(0)
        ).alias("production_orders"),

        F.coalesce(
            F.col("s.order_count"),
            F.lit(0)
        ).alias("semantic_orders"),

        F.coalesce(
            F.col("p.net_sales"),
            F.lit(0.0)
        ).alias("production_net_sales"),

        F.coalesce(
            F.col("s.sales_amount"),
            F.lit(0.0)
        ).alias("semantic_sales"),

        F.coalesce(
            F.col("p.discounts"),
            F.lit(0.0)
        ).alias("production_discounts"),

        F.coalesce(
            F.col("s.discounts"),
            F.lit(0.0)
        ).alias("semantic_discounts")
    )

    .withColumn(
        "order_difference",
        F.abs(
            F.col("production_orders")
            -
            F.col("semantic_orders")
        )
    )

    .withColumn(
        "sales_difference",
        F.abs(
            F.col("production_net_sales")
            -
            F.col("semantic_sales")
        )
    )

    .withColumn(
        "discount_difference",
        F.abs(
            F.col("production_discounts")
            -
            F.col("semantic_discounts")
        )
    )
)


daily_order_diff = daily_comparison.filter(
    F.col("order_difference")
    > F.lit(COUNT_TOLERANCE)
)

daily_sales_diff = daily_comparison.filter(
    F.col("sales_difference")
    > F.lit(AMOUNT_TOLERANCE)
)

daily_discount_diff = daily_comparison.filter(
    F.col("discount_difference")
    > F.lit(AMOUNT_TOLERANCE)
)


print(
    f"Daily dates checked       : "
    f"{daily_comparison.count():,}"
)

print(
    f"Daily order differences   : "
    f"{daily_order_diff.count():,}"
)

print(
    f"Daily sales differences   : "
    f"{daily_sales_diff.count():,}"
)

print(
    f"Daily discount differences: "
    f"{daily_discount_diff.count():,}"
)


if daily_order_diff.count() == 0:

    print("✅ Daily order counts reconcile")

else:

    print("⚠️ Daily order differences detected")

    daily_order_diff.show(
        30,
        truncate=False
    )


if daily_discount_diff.count() == 0:

    print("✅ Daily discounts reconcile")

else:

    print("⚠️ Daily discount differences detected")

    daily_discount_diff.show(
        30,
        truncate=False
    )


# Sales are intentionally diagnostic because:
#
# production = total_net_sales
# semantic   = totalProductPrice
#
# They should NOT automatically be treated as the same metric.

if daily_sales_diff.count() == 0:

    print(
        "✅ Daily production net sales and semantic sales "
        "currently reconcile"
    )

else:

    print(
        "ℹ️ Daily sales differences detected."
    )

    daily_sales_diff.orderBy(
        F.desc("sales_difference")
    ).show(
        30,
        truncate=False
    )


# ================================================================================
# 10. STOCK VALIDATION
# ================================================================================

print("\n[10/12] Validating stock...")


production_stock = spark.sql(
    f"""
    SELECT

        COALESCE(
            SUM(quantity),
            0.0
        ) AS quantity,

        COALESCE(
            SUM(total_amount),
            0.0
        ) AS stock_value,

        COUNT(*) AS row_count

    FROM {GOLD_STOCK}
    """
).first()


semantic_stock = spark.sql(
    f"""
    SELECT

        COALESCE(
            SUM(quantity),
            0.0
        ) AS quantity,

        COALESCE(
            SUM(total_amount),
            0.0
        ) AS stock_value,

        COUNT(*) AS row_count

    FROM {VW_STOCK}
    """
).first()


print(
    "\nSTOCK GLOBAL RECONCILIATION"
)

print_comparison(
    "Stock quantity",
    production_stock["quantity"],
    semantic_stock["quantity"],
    QUANTITY_TOLERANCE
)

print_comparison(
    "Stock value",
    production_stock["stock_value"],
    semantic_stock["stock_value"],
    AMOUNT_TOLERANCE
)


print(
    f"Production stock rows : "
    f"{production_stock['row_count']:,}"
)

print(
    f"Semantic stock rows   : "
    f"{semantic_stock['row_count']:,}"
)


if (
    production_stock["row_count"]
    ==
    semantic_stock["row_count"]
):

    print("✅ Stock row count reconciles")

else:

    print(
        "ℹ️ Stock row counts differ."
    )


# ================================================================================
# 11. RTV VALIDATION
# ================================================================================

print("\n[11/12] Validating RTV...")


production_rtv = spark.sql(
    f"""
    SELECT

        COALESCE(
            SUM(quantity),
            0.0
        ) AS quantity,

        COALESCE(
            SUM(totalAmount),
            0.0
        ) AS amount,

        COUNT(*) AS row_count

    FROM {GOLD_RTV}
    """
).first()


semantic_rtv = spark.sql(
    f"""
    SELECT

        COALESCE(
            SUM(quantity),
            0.0
        ) AS quantity,

        COALESCE(
            SUM(total_amount),
            0.0
        ) AS amount,

        COUNT(*) AS row_count

    FROM {VW_RTV}
    """
).first()


print(
    "\nRTV GLOBAL RECONCILIATION"
)

print_comparison(
    "RTV quantity",
    production_rtv["quantity"],
    semantic_rtv["quantity"],
    QUANTITY_TOLERANCE
)

print_comparison(
    "RTV amount",
    production_rtv["amount"],
    semantic_rtv["amount"],
    AMOUNT_TOLERANCE
)


print(
    f"Production RTV rows : "
    f"{production_rtv['row_count']:,}"
)

print(
    f"Semantic RTV rows   : "
    f"{semantic_rtv['row_count']:,}"
)


if (
    production_rtv["row_count"]
    ==
    semantic_rtv["row_count"]
):

    print("✅ RTV row count reconciles")

else:

    print(
        "ℹ️ RTV row counts differ."
    )


# ================================================================================
# 12. GRN VALIDATION
# ================================================================================

print("\n[12/12] Validating GRN...")


production_grn = spark.sql(
    f"""
    SELECT

        COALESCE(
            SUM(recievedQuantity),
            0
        ) AS received_quantity,

        COALESCE(
            SUM(totalCost),
            0.0
        ) AS total_cost,

        COALESCE(
            SUM(amountNet),
            0.0
        ) AS amount_net,

        COUNT(*) AS row_count

    FROM {GOLD_GRN}
    """
).first()


semantic_grn = spark.sql(
    f"""
    SELECT

        COALESCE(
            SUM(received_quantity),
            0.0
        ) AS received_quantity,

        COALESCE(
            SUM(total_cost),
            0.0
        ) AS total_cost,

        COALESCE(
            SUM(amount_net),
            0.0
        ) AS amount_net,

        COUNT(*) AS row_count

    FROM {VW_GRN}
    """
).first()


print(
    "\nGRN GLOBAL RECONCILIATION"
)

print_comparison(
    "GRN received quantity",
    production_grn["received_quantity"],
    semantic_grn["received_quantity"],
    QUANTITY_TOLERANCE
)

print_comparison(
    "GRN total cost",
    production_grn["total_cost"],
    semantic_grn["total_cost"],
    AMOUNT_TOLERANCE
)

print_comparison(
    "GRN amount net",
    production_grn["amount_net"],
    semantic_grn["amount_net"],
    AMOUNT_TOLERANCE
)


print(
    f"Production GRN rows : "
    f"{production_grn['row_count']:,}"
)

print(
    f"Semantic GRN rows   : "
    f"{semantic_grn['row_count']:,}"
)


if (
    production_grn["row_count"]
    ==
    semantic_grn["row_count"]
):

    print("✅ GRN row count reconciles")

else:

    print(
        "ℹ️ GRN row counts differ."
    )


# ================================================================================
# 13. VALIDATION SUMMARY
# ================================================================================

print("\n")
print("=" * 80)
print("SEMANTIC MIGRATION VALIDATION SUMMARY")
print("=" * 80)


print(
    """
VALIDATED:

    ✅ Semantic views exist
    ✅ Sales semantic layer is populated
    ✅ Store performance comparison performed
    ✅ Brand performance comparison performed
    ✅ Product performance comparison performed
    ✅ Category comparison performed
    ✅ Daily sales comparison performed
    ✅ Stock comparison performed
    ✅ RTV comparison performed
    ✅ GRN comparison performed


IMPORTANT DEFINITION DIFFERENCES:

    1. Production gold_category_performance uses:
           subCategoryOf

       Semantic category view uses:
           category_name

       Therefore category equivalence must be confirmed separately.


    2. Production gold_daily_sales uses:
           total_net_sales

       Semantic sales uses:
           totalProductPrice

       These are treated as different metrics unless reconciliation
       proves that they are equivalent.


    3. Production Gold tables and semantic views can have different
       physical grains. Therefore row-count differences alone do not
       necessarily indicate a migration problem.


NEXT STEP:

    Investigate any reported differences before moving to:

        27_weekly_cache
        28_monthly_cache
        29_llm_reports
        30_distribution
        31_dashboard
"""
)


print("=" * 80)
print("26_VALIDATE_SEMANTIC_LAYER COMPLETED")
print("=" * 80)

# COMMAND ----------


spark.table(TARGET_TABLE).select(
    "price",
    "quantity",
    "total_amount"
).printSchema()

# Incoming Silver schema
silver.select(
    "price",
    "quantity",
    "total_amount"
).printSchema()

# COMMAND ----------

