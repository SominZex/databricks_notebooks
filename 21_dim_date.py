# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# ============================================================
# 21_dim_date
# CANONICAL DATE DIMENSION
#
# Target:
#   billing.sales_data.dim_date
#
# Grain:
#   1 row per calendar date
#
# Range:
#   2024-01-01 through 2030-12-31
#
# Week definition:
#   Monday = first day of week
#
# Designed for:
#   - Daily reporting
#   - Weekly reporting
#   - Monthly reporting
#   - Yearly reporting
#   - WoW / MoM analysis
#   - Databricks SQL / BI
# ============================================================

from pyspark.sql import functions as F
from datetime import date


# ============================================================
# 1. CONFIG
# ============================================================

CATALOG = "billing"
SCHEMA = "sales_data"

TARGET_TABLE = f"{CATALOG}.{SCHEMA}.dim_date"

START_DATE = "2024-01-01"
END_DATE = "2030-12-31"


print("=" * 80)
print("21 DIM DATE")
print("=" * 80)

print(f"Target     : {TARGET_TABLE}")
print(f"Start date : {START_DATE}")
print(f"End date   : {END_DATE}")


# ============================================================
# 2. GENERATE DATE RANGE
# ============================================================

date_df = (
    spark.range(
        1
    )
    .select(
        F.explode(
            F.sequence(
                F.to_date(F.lit(START_DATE)),
                F.to_date(F.lit(END_DATE)),
                F.expr("INTERVAL 1 DAY")
            )
        ).alias("date")
    )
)


# ============================================================
# 3. BASIC DATE ATTRIBUTES
# ============================================================

date_df = (
    date_df

    # Surrogate integer key
    .withColumn(
        "date_key",
        F.date_format(
            F.col("date"),
            "yyyyMMdd"
        ).cast("int")
    )

    # Calendar
    .withColumn(
        "year",
        F.year("date")
    )

    .withColumn(
        "quarter",
        F.quarter("date")
    )

    .withColumn(
        "quarter_name",
        F.concat(
            F.lit("Q"),
            F.quarter("date")
        )
    )

    .withColumn(
        "month",
        F.month("date")
    )

    .withColumn(
        "month_name",
        F.date_format(
            "date",
            "MMMM"
        )
    )

    .withColumn(
        "month_short_name",
        F.date_format(
            "date",
            "MMM"
        )
    )

    .withColumn(
        "year_month",
        F.date_format(
            "date",
            "yyyy-MM"
        )
    )

    .withColumn(
        "year_month_key",
        (
            F.year("date") * 100
            + F.month("date")
        )
    )

    .withColumn(
        "day_of_month",
        F.dayofmonth("date")
    )

    .withColumn(
        "day_of_year",
        F.dayofyear("date")
    )

    .withColumn(
        "day_of_week",
        F.dayofweek("date")
    )

    .withColumn(
        "day_name",
        F.date_format(
            "date",
            "EEEE"
        )
    )

    .withColumn(
        "day_short_name",
        F.date_format(
            "date",
            "EEE"
        )
    )
)


# ============================================================
# 4. MONDAY-BASED WEEK ATTRIBUTES
# ============================================================
#
# Spark dayofweek():
#
# Sunday = 1
# Monday = 2
# ...
# Saturday = 7
#
# We convert that into:
#
# Monday = 1
# Tuesday = 2
# ...
# Sunday = 7
# ============================================================

date_df = (
    date_df

    .withColumn(
        "iso_day_of_week",
        F.when(
            F.dayofweek("date") == 1,
            F.lit(7)
        )
        .otherwise(
            F.dayofweek("date") - 1
        )
    )

    .withColumn(
        "week_start_date",
        F.date_sub(
            F.col("date"),
            F.col("iso_day_of_week") - 1
        )
    )

    .withColumn(
        "week_end_date",
        F.date_add(
            F.col("week_start_date"),
            6
        )
    )

    .withColumn(
        "week_of_year",
        F.weekofyear("date")
    )

    .withColumn(
        "year_week",
        F.concat(
            F.year("date"),
            F.lit("-W"),
            F.lpad(
                F.weekofyear("date").cast("string"),
                2,
                "0"
            )
        )
    )
)


# ============================================================
# 5. MONTH BOUNDARIES
# ============================================================

date_df = (
    date_df

    .withColumn(
        "month_start_date",
        F.trunc(
            F.col("date"),
            "month"
        )
    )

    .withColumn(
        "month_end_date",
        F.last_day(
            F.col("date")
        )
    )

    .withColumn(
        "days_in_month",
        F.dayofmonth(
            F.last_day("date")
        )
    )

    .withColumn(
        "month_day_number",
        F.dayofmonth("date")
    )
)


# ============================================================
# 6. QUARTER BOUNDARIES
# ============================================================

date_df = (
    date_df

    .withColumn(
        "quarter_start_date",
        F.expr(
            """
            make_date(
                year(date),
                ((quarter(date) - 1) * 3) + 1,
                1
            )
            """
        )
    )

    .withColumn(
        "quarter_end_date",
        F.last_day(
            F.add_months(
                F.col("quarter_start_date"),
                2
            )
        )
    )
)


# ============================================================
# 7. YEAR BOUNDARIES
# ============================================================

date_df = (
    date_df

    .withColumn(
        "year_start_date",
        F.make_date(
            F.year("date"),
            F.lit(1),
            F.lit(1)
        )
    )

    .withColumn(
        "year_end_date",
        F.make_date(
            F.year("date"),
            F.lit(12),
            F.lit(31)
        )
    )
)


# ============================================================
# 8. BUSINESS / WEEKEND FLAGS
# ============================================================

date_df = (
    date_df

    .withColumn(
        "is_weekend",
        F.col("iso_day_of_week").isin(
            6,
            7
        )
    )

    .withColumn(
        "is_weekday",
        ~F.col("is_weekend")
    )
)


# ============================================================
# 9. MONTH / QUARTER POSITION
# ============================================================

date_df = (
    date_df

    .withColumn(
        "month_position",
        F.when(
            F.month("date") == 1,
            "January"
        )
        .otherwise(
            F.lit(None)
        )
    )
)


# ============================================================
# 10. CLEAN MONTH POSITION
# ============================================================
#
# Instead of storing unnecessary textual labels, provide
# numeric positions useful for BI sorting.
# ============================================================

date_df = (
    date_df

    .withColumn(
        "month_number",
        F.month("date")
    )

    .withColumn(
        "quarter_number",
        F.quarter("date")
    )
)


# ============================================================
# 11. FINAL COLUMN ORDER
# ============================================================

date_df = (
    date_df
    .select(
        # Keys
        "date_key",
        "date",

        # Calendar
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

        # Day
        "day_of_month",
        "day_of_year",
        "day_of_week",
        "day_name",
        "day_short_name",
        "iso_day_of_week",

        # Week
        "week_of_year",
        "year_week",
        "week_start_date",
        "week_end_date",

        # Month boundaries
        "month_start_date",
        "month_end_date",
        "days_in_month",
        "month_day_number",

        # Quarter boundaries
        "quarter_start_date",
        "quarter_end_date",

        # Year boundaries
        "year_start_date",
        "year_end_date",

        # Business flags
        "is_weekend",
        "is_weekday"
    )
)


# ============================================================
# 12. REMOVE UNUSED COLUMN
# ============================================================

# month_position was only an intermediate expression.
if "month_position" in date_df.columns:
    date_df = date_df.drop("month_position")


# ============================================================
# 13. PRE-WRITE VALIDATION
# ============================================================

row_count = date_df.count()

expected_count = (
    F.datediff(
        F.to_date(F.lit(END_DATE)),
        F.to_date(F.lit(START_DATE))
    )
    + 1
)

expected_count_value = (
    spark.range(1)
    .select(expected_count.alias("count"))
    .collect()[0]["count"]
)

if row_count != expected_count_value:
    raise RuntimeError(
        f"❌ Date row count mismatch. "
        f"Expected {expected_count_value}, "
        f"got {row_count}"
    )


# ============================================================
# 14. DATE KEY VALIDATION
# ============================================================

null_date_keys = (
    date_df
    .filter(
        F.col("date_key").isNull()
    )
    .count()
)

if null_date_keys > 0:
    raise RuntimeError(
        f"❌ {null_date_keys} NULL date keys."
    )


duplicate_date_keys = (
    date_df
    .groupBy("date_key")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

if duplicate_date_keys > 0:
    raise RuntimeError(
        f"❌ {duplicate_date_keys} duplicate date keys."
    )


# ============================================================
# 15. WEEK VALIDATION
# ============================================================

invalid_week_starts = (
    date_df
    .filter(
        F.dayofweek(
            F.col("week_start_date")
        ) != 2
    )
    .count()
)

if invalid_week_starts > 0:
    raise RuntimeError(
        f"❌ {invalid_week_starts} dates have "
        f"a non-Monday week_start_date."
    )


invalid_week_ends = (
    date_df
    .filter(
        F.dayofweek(
            F.col("week_end_date")
        ) != 1
    )
    .count()
)

if invalid_week_ends > 0:
    raise RuntimeError(
        f"❌ {invalid_week_ends} dates have "
        f"a non-Sunday week_end_date."
    )


# ============================================================
# 16. MONTH VALIDATION
# ============================================================

invalid_month_ranges = (
    date_df
    .filter(
        (F.col("date") < F.col("month_start_date"))
        |
        (F.col("date") > F.col("month_end_date"))
    )
    .count()
)

if invalid_month_ranges > 0:
    raise RuntimeError(
        f"❌ {invalid_month_ranges} invalid month ranges."
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
        "Rebuilding deterministic date dimension..."
    )

    spark.sql(
        f"DROP TABLE {TARGET_TABLE}"
    )

    print(
        "✅ Existing dim_date removed."
    )


# ============================================================
# 18. WRITE DELTA TABLE
# ============================================================

(
    date_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

print(
    "✅ dim_date created successfully"
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
    f"Total dates: {final_count:,}"
)

if final_count != expected_count_value:
    raise RuntimeError(
        f"❌ Final date count mismatch. "
        f"Expected {expected_count_value}, "
        f"got {final_count}"
    )


# ============================================================
# 20. FINAL DATE KEY VALIDATION
# ============================================================

final_duplicate_keys = (
    final_df
    .groupBy("date_key")
    .count()
    .filter(
        F.col("count") > 1
    )
    .count()
)

if final_duplicate_keys > 0:
    raise RuntimeError(
        f"❌ Final table contains "
        f"{final_duplicate_keys} duplicate date keys."
    )


# ============================================================
# 21. FINAL DATE RANGE
# ============================================================

actual_min_date = (
    final_df
    .agg(F.min("date"))
    .collect()[0][0]
)

actual_max_date = (
    final_df
    .agg(F.max("date"))
    .collect()[0][0]
)

expected_min_date = date.fromisoformat(
    START_DATE
)

expected_max_date = date.fromisoformat(
    END_DATE
)

if actual_min_date != expected_min_date:
    raise RuntimeError(
        f"❌ Incorrect minimum date: "
        f"{actual_min_date}"
    )

if actual_max_date != expected_max_date:
    raise RuntimeError(
        f"❌ Incorrect maximum date: "
        f"{actual_max_date}"
    )


# ============================================================
# 22. FINAL WEEKEND VALIDATION
# ============================================================

weekend_mismatch = (
    final_df
    .filter(
        (
            F.col("iso_day_of_week").isin(6, 7)
        )
        != F.col("is_weekend")
    )
    .count()
)

if weekend_mismatch > 0:
    raise RuntimeError(
        f"❌ {weekend_mismatch} weekend flag mismatches."
    )


print("✅ Final date_key validation passed")
print("✅ Final date range validation passed")
print("✅ Final weekend validation passed")


# ============================================================
# 23. SAMPLE
# ============================================================

display(
    final_df
    .orderBy("date")
    .limit(30)
)


# ============================================================
# 24. FINAL SUCCESS
# ============================================================

print()
print("=" * 80)
print("21 DIM DATE SUCCESS")
print("=" * 80)

print(
    f"Date rows       : {final_count:,}"
)

print(
    f"Date range      : {actual_min_date} → {actual_max_date}"
)

print(
    f"Week definition : Monday → Sunday"
)

print(
    f"Target          : {TARGET_TABLE}"
)

print("=" * 80)

# COMMAND ----------

