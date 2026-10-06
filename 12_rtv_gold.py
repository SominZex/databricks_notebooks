# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# =============================================================================
# PRODUCTION-GRADE GOLD RTV PIPELINE
#
# Source : <catalog_name>.<schema_name>.rtv_silver
# Target : <catalog_name>.<schema_name>.gold_rtv
# Audit  : <catalog_name>.<schema_name>.gold_rtv_pipeline_audit
#
# Requirements:
#   - Existing SparkSession named `spark`
#   - Existing notebook/job variables: catalog_name, schema_name
#   - Delta Lake tables
#
# Design:
#   1. Select the latest valid Silver ingestion batch.
#   2. Validate metadata and business data before changing Gold.
#   3. Process only the selected business-date window.
#   4. Deduplicate deterministic exact duplicates.
#   5. Use source_window_key as the authoritative replacement scope.
#   6. Never delete all records solely by source_date.
#   7. Track execution in an audit table.
#   8. Validate the written Gold scope and reconcile counts/totals.
#
# Operational recommendation:
#   Configure the Databricks job to allow only one concurrent run of this task.
#   The audit table also rejects a concurrent RUNNING execution for the same
#   source_window_key.
# =============================================================================

from datetime import datetime, date, timedelta, timezone
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    coalesce,
    to_date,
    count,
    sum as spark_sum,
    min as spark_min,
    max as spark_max,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    TimestampType,
    LongType,
    DoubleType,
)


# =============================================================================
# 1. CONFIGURATION
# =============================================================================

SOURCE_TABLE = f"{catalog_name}.{schema_name}.rtv_silver"
TARGET_TABLE = f"{catalog_name}.{schema_name}.gold_rtv"
AUDIT_TABLE = f"{catalog_name}.{schema_name}.gold_rtv_pipeline_audit"

PIPELINE_RUN_ID = str(uuid4())
PIPELINE_NAME = "gold_rtv_pipeline"

REQUIRED_SILVER_COLUMNS = [
    "rtvId",
    "Date",
    "time",
    "vendorName",
    "productId",
    "productName",
    "barcode",
    "quantity",
    "price",
    "totalAmount",
    "description",
    "storeName",
    "doneBy",
]

REQUIRED_METADATA_COLUMNS = [
    "_run_id",
    "execution_date",
    "from_date",
    "to_date",
]

DEDUPLICATION_COLUMNS = [
    "rtvId",
    "Date",
    "time",
    "productId",
    "barcode",
    "storeName",
    "quantity",
    "price",
    "totalAmount",
]

LINEAGE_COLUMNS = [
    "source_date",
    "source_run_id",
    "source_execution_date",
    "source_window_start",
    "source_window_end",
    "source_window_key",
    "gold_ingested_at",
]


# =============================================================================
# 2. HELPERS
# =============================================================================

def parse_date_column(column_name):
    """Return a Spark DATE expression for supported source date formats."""
    date_as_string = trim(col(column_name).cast("string"))

    return (
        F.when(
            date_as_string.rlike(r"^\d{4}-\d{2}-\d{2}$"),
            F.to_date(date_as_string, "yyyy-MM-dd"),
        )
        .when(
            date_as_string.rlike(r"^\d{4}/\d{1,2}/\d{1,2}$"),
            F.to_date(date_as_string, "yyyy/M/d"),
        )
        .when(
            date_as_string.rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"),
            F.to_date(date_as_string, "M/d/yyyy"),
        )
        .when(
            date_as_string.rlike(r"^\d{1,2}/\d{1,2}/\d{2}$"),
            F.to_date(date_as_string, "M/d/yy"),
        )
        .otherwise(F.lit(None).cast("date"))
    )


def parse_python_date(value, field_name):
    """Convert a Spark/Python date-like value into datetime.date."""
    if value is None:
        raise RuntimeError(f"{field_name} is NULL.")

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    if isinstance(value, str):
        value = value.strip()
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                pass

    raise RuntimeError(f"Could not parse {field_name}: {value!r}")


def sql_escape(value):
    """Escape a string for a SQL string literal."""
    return str(value).replace("'", "''")


def date_sql(value):
    return value.strftime("%Y-%m-%d")


def table_exists(table_name):
    return spark.catalog.tableExists(table_name)


def ensure_audit_table():
    """Create the audit table if it does not exist."""
    if table_exists(AUDIT_TABLE):
        return

    audit_schema = StructType([
        StructField("pipeline_run_id", StringType(), False),
        StructField("pipeline_name", StringType(), False),
        StructField("source_run_id", StringType(), True),
        StructField("source_window_key", StringType(), True),
        StructField("window_start", DateType(), True),
        StructField("window_end_exclusive", DateType(), True),
        StructField("status", StringType(), False),
        StructField("started_at", TimestampType(), False),
        StructField("completed_at", TimestampType(), True),
        StructField("source_rows_available", LongType(), True),
        StructField("selected_batch_rows", LongType(), True),
        StructField("rows_before_window", LongType(), True),
        StructField("rows_in_window", LongType(), True),
        StructField("rows_before_dedup", LongType(), True),
        StructField("rows_after_dedup", LongType(), True),
        StructField("rows_written", LongType(), True),
        StructField("duplicates_removed", LongType(), True),
        StructField("total_quantity", DoubleType(), True),
        StructField("total_value", DoubleType(), True),
        StructField("error_message", StringType(), True),
    ])

    empty_df = spark.createDataFrame([], audit_schema)

    (
        empty_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(AUDIT_TABLE)
    )


def audit_insert(
    source_run_id,
    source_window_key,
    window_start,
    window_end_exclusive,
    status,
    started_at,
    completed_at=None,
    source_rows_available=None,
    selected_batch_rows=None,
    rows_before_window=None,
    rows_in_window=None,
    rows_before_dedup=None,
    rows_after_dedup=None,
    rows_written=None,
    duplicates_removed=None,
    total_quantity=None,
    total_value=None,
    error_message=None,
):
    """Append one audit record."""
    row = [(
        PIPELINE_RUN_ID,
        PIPELINE_NAME,
        source_run_id,
        source_window_key,
        window_start,
        window_end_exclusive,
        status,
        started_at,
        completed_at,
        source_rows_available,
        selected_batch_rows,
        rows_before_window,
        rows_in_window,
        rows_before_dedup,
        rows_after_dedup,
        rows_written,
        duplicates_removed,
        total_quantity,
        total_value,
        error_message,
    )]

    audit_schema = StructType([
        StructField("pipeline_run_id", StringType(), False),
        StructField("pipeline_name", StringType(), False),
        StructField("source_run_id", StringType(), True),
        StructField("source_window_key", StringType(), True),
        StructField("window_start", DateType(), True),
        StructField("window_end_exclusive", DateType(), True),
        StructField("status", StringType(), False),
        StructField("started_at", TimestampType(), False),
        StructField("completed_at", TimestampType(), True),
        StructField("source_rows_available", LongType(), True),
        StructField("selected_batch_rows", LongType(), True),
        StructField("rows_before_window", LongType(), True),
        StructField("rows_in_window", LongType(), True),
        StructField("rows_before_dedup", LongType(), True),
        StructField("rows_after_dedup", LongType(), True),
        StructField("rows_written", LongType(), True),
        StructField("duplicates_removed", LongType(), True),
        StructField("total_quantity", DoubleType(), True),
        StructField("total_value", DoubleType(), True),
        StructField("error_message", StringType(), True),
    ])

    (
        spark.createDataFrame(row, audit_schema)
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(AUDIT_TABLE)
    )


def audit_update(status, completed_at=None, error_message=None):
    """Update the current execution status in the audit table."""
    assignments = [f"status = '{sql_escape(status)}'"]

    if completed_at is not None:
        assignments.append(
            f"completed_at = TIMESTAMP '{completed_at.strftime('%Y-%m-%d %H:%M:%S')}'"
        )

    if error_message is not None:
        assignments.append(
            f"error_message = '{sql_escape(error_message[:4000])}'"
        )

    spark.sql(
        f"""
        UPDATE {AUDIT_TABLE}
        SET {", ".join(assignments)}
        WHERE pipeline_run_id = '{sql_escape(PIPELINE_RUN_ID)}'
        """
    )


def ensure_lineage_columns():
    """Add only missing lineage columns to an existing Gold table."""
    expected_types = {
        "source_run_id": "STRING",
        "source_execution_date": "DATE",
        "source_window_start": "DATE",
        "source_window_end": "DATE",
        "source_window_key": "STRING",
        "gold_ingested_at": "TIMESTAMP",
    }

    existing_columns = set(spark.table(TARGET_TABLE).columns)

    if "source_date" not in existing_columns:
        raise RuntimeError(
            f"Existing target {TARGET_TABLE} does not contain source_date. "
            "The table is incompatible with the required partition schema."
        )

    for column_name, data_type in expected_types.items():
        if column_name not in existing_columns:
            spark.sql(
                f"ALTER TABLE {TARGET_TABLE} "
                f"ADD COLUMNS ({column_name} {data_type})"
            )


def validate_no_active_same_scope(source_window_key):
    """Reject a concurrent RUNNING execution for the same logical scope."""
    active_count = (
        spark.table(AUDIT_TABLE)
        .filter(
            (col("pipeline_name") == lit(PIPELINE_NAME))
            & (col("source_window_key") == lit(source_window_key))
            & (col("status") == lit("RUNNING"))
            & (col("pipeline_run_id") != lit(PIPELINE_RUN_ID))
        )
        .count()
    )

    if active_count > 0:
        raise RuntimeError(
            f"Another RUNNING pipeline execution exists for "
            f"source_window_key={source_window_key}."
        )


def validate_required_columns(actual_columns, required_columns, label):
    missing = [c for c in required_columns if c not in actual_columns]
    if missing:
        raise RuntimeError(f"{label} is missing required columns: {missing}")


# =============================================================================
# 3. STARTUP
# =============================================================================

started_at = datetime.now(timezone.utc)
ensure_audit_table()

print("=" * 110)
print("PRODUCTION GOLD RTV PIPELINE")
print("=" * 110)
print(f"Pipeline run ID : {PIPELINE_RUN_ID}")
print(f"Source table    : {SOURCE_TABLE}")
print(f"Target table    : {TARGET_TABLE}")
print(f"Audit table     : {AUDIT_TABLE}")
print("=" * 110)


# Runtime values used by the failure handler.
SOURCE_RUN_ID = None
SOURCE_WINDOW_KEY = None
WINDOW_START = None
WINDOW_END_EXCLUSIVE = None
source_rows_available = None
selected_batch_rows = None
rows_before_window = None
rows_in_window = None
rows_before_dedup = None
rows_after_dedup = None
duplicates_removed = None
written_count = None
total_qty = None
total_value = None


try:
    # =========================================================================
    # 4. VALIDATE SOURCE
    # =========================================================================

    if not table_exists(SOURCE_TABLE):
        raise RuntimeError(f"Source table does not exist: {SOURCE_TABLE}")

    silver = spark.table(SOURCE_TABLE)
    silver_columns = silver.columns
    source_rows_available = silver.count()

    if source_rows_available == 0:
        raise RuntimeError("Silver table contains no rows.")

    validate_required_columns(
        silver_columns,
        REQUIRED_SILVER_COLUMNS,
        "Silver data"
    )
    validate_required_columns(
        silver_columns,
        REQUIRED_METADATA_COLUMNS,
        "Silver metadata"
    )

    print(f"Silver rows available: {source_rows_available:,}")

    # =========================================================================
    # 5. NORMALIZE AND VALIDATE METADATA
    # =========================================================================

    metadata_select = [
        "_run_id",
        "execution_date",
        "from_date",
        "to_date",
    ]

    for optional_column in ("ingested_at", "ingestion_mode", "week_start", "week_end"):
        if optional_column in silver_columns:
            metadata_select.append(optional_column)

    silver_metadata = (
        silver.select(*metadata_select)
        .withColumn("_run_id", trim(col("_run_id").cast("string")))
        .withColumn("_metadata_execution_date", parse_date_column("execution_date"))
        .withColumn("_metadata_from_date", parse_date_column("from_date"))
        .withColumn("_metadata_to_date", parse_date_column("to_date"))
    )

    invalid_metadata_count = (
        silver_metadata
        .filter(
            col("_run_id").isNull()
            | (trim(col("_run_id")) == "")
            | col("_metadata_execution_date").isNull()
            | col("_metadata_from_date").isNull()
            | col("_metadata_to_date").isNull()
            | (col("_metadata_from_date") >= col("_metadata_to_date"))
        )
        .count()
    )

    if invalid_metadata_count > 0:
        raise RuntimeError(
            f"Found {invalid_metadata_count:,} rows with invalid ingestion metadata."
        )

    # =========================================================================
    # 6. SELECT LATEST VALID BATCH
    # =========================================================================

    order_columns = [col("_metadata_execution_date").desc()]

    if "ingested_at" in silver_columns:
        order_columns.append(col("ingested_at").desc_nulls_last())

    order_columns.append(col("_run_id").desc())

    latest_batch = (
        silver_metadata
        .select(*metadata_select, "_metadata_execution_date",
                "_metadata_from_date", "_metadata_to_date")
        .dropDuplicates([
            "_run_id",
            "_metadata_execution_date",
            "_metadata_from_date",
            "_metadata_to_date",
        ])
        .orderBy(*order_columns)
        .limit(1)
        .collect()
    )

    if not latest_batch:
        raise RuntimeError("No valid Silver ingestion batch was found.")

    batch = latest_batch[0].asDict()

    SOURCE_RUN_ID = str(batch["_run_id"]).strip()
    EXECUTION_DATE = parse_python_date(
        batch["_metadata_execution_date"], "execution_date"
    )
    WINDOW_START = parse_python_date(
        batch["_metadata_from_date"], "from_date"
    )
    WINDOW_END_EXCLUSIVE = parse_python_date(
        batch["_metadata_to_date"], "to_date"
    )
    WINDOW_END_INCLUSIVE = WINDOW_END_EXCLUSIVE - timedelta(days=1)

    if WINDOW_START >= WINDOW_END_EXCLUSIVE:
        raise RuntimeError("Invalid processing window.")

    SOURCE_WINDOW_KEY = (
        f"{WINDOW_START:%Y-%m-%d}__{WINDOW_END_EXCLUSIVE:%Y-%m-%d}"
    )

    validate_no_active_same_scope(SOURCE_WINDOW_KEY)

    audit_insert(
        source_run_id=SOURCE_RUN_ID,
        source_window_key=SOURCE_WINDOW_KEY,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END_EXCLUSIVE,
        status="RUNNING",
        started_at=started_at,
    )

    print(f"Selected source run ID : {SOURCE_RUN_ID}")
    print(f"Execution date         : {EXECUTION_DATE}")
    print(f"Window start           : {WINDOW_START}")
    print(f"Window end exclusive   : {WINDOW_END_EXCLUSIVE}")
    print(f"Window key             : {SOURCE_WINDOW_KEY}")

    # =========================================================================
    # 7. SELECT THE CHOSEN BATCH
    # =========================================================================

    selected_batch_df = silver.filter(
        trim(col("_run_id").cast("string")) == lit(SOURCE_RUN_ID)
    )

    selected_batch_rows = selected_batch_df.count()

    if selected_batch_rows == 0:
        raise RuntimeError("Selected Silver run ID contains no rows.")

    # =========================================================================
    # 8. NORMALIZE RTV DATA
    # =========================================================================

    rtv_df = selected_batch_df.select(*REQUIRED_SILVER_COLUMNS)

    for string_column in (
        "rtvId",
        "time",
        "vendorName",
        "productId",
        "productName",
        "barcode",
        "description",
        "storeName",
        "doneBy",
    ):
        rtv_df = rtv_df.withColumn(
            string_column,
            trim(col(string_column).cast("string"))
        )

    for numeric_column in ("quantity", "price", "totalAmount"):
        rtv_df = rtv_df.withColumn(
            numeric_column,
            col(numeric_column).cast("double")
        )

    rtv_df = rtv_df.withColumn("Date", parse_date_column("Date"))

    # =========================================================================
    # 9. DATA QUALITY VALIDATION
    # =========================================================================

    invalid_date_count = rtv_df.filter(col("Date").isNull()).count()
    if invalid_date_count > 0:
        raise RuntimeError(
            f"{invalid_date_count:,} records have NULL or invalid Date values."
        )

    invalid_store_count = rtv_df.filter(
        col("storeName").isNull() | (trim(col("storeName")) == "")
    ).count()

    if invalid_store_count > 0:
        raise RuntimeError(
            f"{invalid_store_count:,} records have NULL or empty storeName."
        )

    invalid_quantity_count = rtv_df.filter(col("quantity").isNull()).count()
    invalid_amount_count = rtv_df.filter(col("totalAmount").isNull()).count()

    if invalid_quantity_count > 0 or invalid_amount_count > 0:
        raise RuntimeError(
            "Null quantity or totalAmount values detected. "
            f"null_quantity={invalid_quantity_count:,}, "
            f"null_totalAmount={invalid_amount_count:,}."
        )

    rows_before_window = rtv_df.count()

    rtv_df = rtv_df.filter(
        (col("Date") >= lit(WINDOW_START))
        & (col("Date") < lit(WINDOW_END_EXCLUSIVE))
    )

    rows_in_window = rtv_df.count()

    if rows_in_window == 0:
        raise RuntimeError("No RTV records exist inside the selected window.")

    # =========================================================================
    # 10. ADD LINEAGE AND PARTITION COLUMNS
    # =========================================================================

    rtv_df = (
        rtv_df
        .withColumn("source_date", lit(WINDOW_END_INCLUSIVE).cast("date"))
        .withColumn("source_run_id", lit(SOURCE_RUN_ID).cast("string"))
        .withColumn("source_execution_date", lit(EXECUTION_DATE).cast("date"))
        .withColumn("source_window_start", lit(WINDOW_START).cast("date"))
        .withColumn("source_window_end", lit(WINDOW_END_EXCLUSIVE).cast("date"))
        .withColumn("source_window_key", lit(SOURCE_WINDOW_KEY).cast("string"))
        .withColumn("gold_ingested_at", F.current_timestamp())
    )

    # =========================================================================
    # 11. DEDUPLICATION
    # =========================================================================

    rows_before_dedup = rtv_df.count()

    rtv_df = rtv_df.dropDuplicates(DEDUPLICATION_COLUMNS)

    rows_after_dedup = rtv_df.count()
    duplicates_removed = rows_before_dedup - rows_after_dedup

    if rows_after_dedup == 0:
        raise RuntimeError("No records remain after deduplication.")

    # =========================================================================
    # 12. FINAL PRE-WRITE VALIDATION
    # =========================================================================

    invalid_lineage_count = rtv_df.filter(
        col("source_window_key").isNull()
        | (col("source_window_key") != lit(SOURCE_WINDOW_KEY))
        | (col("source_date") != lit(WINDOW_END_INCLUSIVE))
    ).count()

    if invalid_lineage_count > 0:
        raise RuntimeError(
            f"{invalid_lineage_count:,} records failed lineage validation."
        )

    outside_final_count = rtv_df.filter(
        (col("Date") < lit(WINDOW_START))
        | (col("Date") >= lit(WINDOW_END_EXCLUSIVE))
    ).count()

    if outside_final_count > 0:
        raise RuntimeError(
            f"{outside_final_count:,} records fall outside the processing window."
        )

    # =========================================================================
    # 13. PREPARE TARGET
    # =========================================================================

    target_exists = table_exists(TARGET_TABLE)

    if target_exists:
        ensure_lineage_columns()

        target_columns = set(spark.table(TARGET_TABLE).columns)
        if "source_date" not in target_columns:
            raise RuntimeError(
                "Target table does not contain source_date. "
                "Existing partition compatibility cannot be guaranteed."
            )

        # No broad source_date fallback is used here.
        # Only the exact logical scope is replaced.
        safe_window_key = sql_escape(SOURCE_WINDOW_KEY)

        existing_scope_count = (
            spark.table(TARGET_TABLE)
            .filter(col("source_window_key") == lit(SOURCE_WINDOW_KEY))
            .count()
        )

        print(f"Existing rows for exact scope: {existing_scope_count:,}")

        spark.sql(
            f"""
            DELETE FROM {TARGET_TABLE}
            WHERE source_window_key = '{safe_window_key}'
            """
        )

    # =========================================================================
    # 14. WRITE GOLD
    # =========================================================================

    if target_exists:
        (
            rtv_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "false")
            .saveAsTable(TARGET_TABLE)
        )
    else:
        (
            rtv_df.write
            .format("delta")
            .mode("append")
            .partitionBy("source_date")
            .option("mergeSchema", "false")
            .saveAsTable(TARGET_TABLE)
        )

    # =========================================================================
    # 15. POST-WRITE VALIDATION
    # =========================================================================

    written_df = (
        spark.table(TARGET_TABLE)
        .filter(col("source_window_key") == lit(SOURCE_WINDOW_KEY))
    )

    written_count = written_df.count()

    if written_count != rows_after_dedup:
        raise RuntimeError(
            "Post-write count mismatch. "
            f"expected={rows_after_dedup:,}, actual={written_count:,}."
        )

    validation_conditions = [
        col("source_run_id") != lit(SOURCE_RUN_ID),
        col("source_window_start") != lit(WINDOW_START),
        col("source_window_end") != lit(WINDOW_END_EXCLUSIVE),
        col("source_date") != lit(WINDOW_END_INCLUSIVE),
        (col("Date") < lit(WINDOW_START)),
        (col("Date") >= lit(WINDOW_END_EXCLUSIVE)),
    ]

    invalid_written_condition = validation_conditions[0]
    for validation_condition in validation_conditions[1:]:
        invalid_written_condition = invalid_written_condition | validation_condition

    invalid_written_count = written_df.filter(
        invalid_written_condition
    ).count()

    if invalid_written_count > 0:
        raise RuntimeError(
            f"{invalid_written_count:,} written records failed post-write validation."
        )

    duplicate_group_count = (
        written_df
        .groupBy(*DEDUPLICATION_COLUMNS)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    if duplicate_group_count > 0:
        raise RuntimeError(
            f"{duplicate_group_count:,} duplicate groups detected in Gold scope."
        )

    summary = written_df.agg(
        spark_sum("quantity").alias("total_qty"),
        spark_sum("totalAmount").alias("total_value"),
    ).collect()[0]

    total_qty = float(summary["total_qty"] or 0.0)
    total_value = float(summary["total_value"] or 0.0)

    # =========================================================================
    # 16. SUCCESS AUDIT
    # =========================================================================

    completed_at = datetime.now(timezone.utc)

    audit_update(status="SUCCESS", completed_at=completed_at)


    print("=" * 110)
    print("FINAL GOLD RTV VALIDATION")
    print("=" * 110)
    print(f"Pipeline run ID       : {PIPELINE_RUN_ID}")
    print(f"Source run ID         : {SOURCE_RUN_ID}")
    print(f"Source window key     : {SOURCE_WINDOW_KEY}")
    print(f"Silver rows available : {source_rows_available:,}")
    print(f"Selected batch rows   : {selected_batch_rows:,}")
    print(f"Rows before window    : {rows_before_window:,}")
    print(f"Rows inside window    : {rows_in_window:,}")
    print(f"Rows before dedup     : {rows_before_dedup:,}")
    print(f"Rows after dedup      : {rows_after_dedup:,}")
    print(f"Duplicates removed    : {duplicates_removed:,}")
    print(f"Gold rows written     : {written_count:,}")
    print(f"Total quantity        : {total_qty:,.2f}")
    print(f"Total RTV value       : ₹{total_value:,.2f}")
    print(f"Duplicate groups      : {duplicate_group_count:,}")
    print("=" * 110)
    print("SUCCESS: GOLD RTV PIPELINE COMPLETED")
    print("=" * 110)


except Exception as exc:
    error_message = str(exc)[:4000]

    print("=" * 110)
    print("GOLD RTV PIPELINE FAILED")
    print("=" * 110)
    print(error_message)
    print("=" * 110)

    # Failure audit is best effort. Do not hide the original exception.
    try:
        audit_update(
            status="FAILED",
            completed_at=datetime.now(timezone.utc),
            error_message=error_message,
        )

    except Exception as audit_exc:
        print(f"WARNING: Failed to write failure audit: {audit_exc}")

    raise
# =============================================================================
# PRODUCTION-GRADE GOLD RTV PIPELINE
#
# Source : <catalog_name>.<schema_name>.rtv_silver
# Target : <catalog_name>.<schema_name>.gold_rtv
# Audit  : <catalog_name>.<schema_name>.gold_rtv_pipeline_audit
#
# Requirements:
#   - Existing SparkSession named `spark`
#   - Existing notebook/job variables: catalog_name, schema_name
#   - Delta Lake tables
#
# Design:
#   1. Select the latest valid Silver ingestion batch.
#   2. Validate metadata and business data before changing Gold.
#   3. Process only the selected business-date window.
#   4. Deduplicate deterministic exact duplicates.
#   5. Use source_window_key as the authoritative replacement scope.
#   6. Never delete all records solely by source_date.
#   7. Track execution in an audit table.
#   8. Validate the written Gold scope and reconcile counts/totals.
#
# Operational recommendation:
#   Configure the Databricks job to allow only one concurrent run of this task.
#   The audit table also rejects a concurrent RUNNING execution for the same
#   source_window_key.
# =============================================================================

from datetime import datetime, date, timedelta, timezone
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    coalesce,
    to_date,
    count,
    sum as spark_sum,
    min as spark_min,
    max as spark_max,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    TimestampType,
    LongType,
    DoubleType,
)


# =============================================================================
# 1. CONFIGURATION
# =============================================================================

SOURCE_TABLE = f"{catalog_name}.{schema_name}.rtv_silver"
TARGET_TABLE = f"{catalog_name}.{schema_name}.gold_rtv"
AUDIT_TABLE = f"{catalog_name}.{schema_name}.gold_rtv_pipeline_audit"

PIPELINE_RUN_ID = str(uuid4())
PIPELINE_NAME = "gold_rtv_pipeline"

REQUIRED_SILVER_COLUMNS = [
    "rtvId",
    "Date",
    "time",
    "vendorName",
    "productId",
    "productName",
    "barcode",
    "quantity",
    "price",
    "totalAmount",
    "description",
    "storeName",
    "doneBy",
]

REQUIRED_METADATA_COLUMNS = [
    "_run_id",
    "execution_date",
    "from_date",
    "to_date",
]

DEDUPLICATION_COLUMNS = [
    "rtvId",
    "Date",
    "time",
    "productId",
    "barcode",
    "storeName",
    "quantity",
    "price",
    "totalAmount",
]

LINEAGE_COLUMNS = [
    "source_date",
    "source_run_id",
    "source_execution_date",
    "source_window_start",
    "source_window_end",
    "source_window_key",
    "gold_ingested_at",
]


# =============================================================================
# 2. HELPERS
# =============================================================================

def parse_date_column(column_name):
    """Return a Spark DATE expression for supported source date formats."""
    date_as_string = trim(col(column_name).cast("string"))

    return (
        F.when(
            date_as_string.rlike(r"^\d{4}-\d{2}-\d{2}$"),
            F.to_date(date_as_string, "yyyy-MM-dd"),
        )
        .when(
            date_as_string.rlike(r"^\d{4}/\d{1,2}/\d{1,2}$"),
            F.to_date(date_as_string, "yyyy/M/d"),
        )
        .when(
            date_as_string.rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"),
            F.to_date(date_as_string, "M/d/yyyy"),
        )
        .when(
            date_as_string.rlike(r"^\d{1,2}/\d{1,2}/\d{2}$"),
            F.to_date(date_as_string, "M/d/yy"),
        )
        .otherwise(F.lit(None).cast("date"))
    )


def parse_python_date(value, field_name):
    """Convert a Spark/Python date-like value into datetime.date."""
    if value is None:
        raise RuntimeError(f"{field_name} is NULL.")

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    if isinstance(value, str):
        value = value.strip()
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                pass

    raise RuntimeError(f"Could not parse {field_name}: {value!r}")


def sql_escape(value):
    """Escape a string for a SQL string literal."""
    return str(value).replace("'", "''")


def date_sql(value):
    return value.strftime("%Y-%m-%d")


def table_exists(table_name):
    return spark.catalog.tableExists(table_name)


def ensure_audit_table():
    """Create the audit table if it does not exist."""
    if table_exists(AUDIT_TABLE):
        return

    audit_schema = StructType([
        StructField("pipeline_run_id", StringType(), False),
        StructField("pipeline_name", StringType(), False),
        StructField("source_run_id", StringType(), True),
        StructField("source_window_key", StringType(), True),
        StructField("window_start", DateType(), True),
        StructField("window_end_exclusive", DateType(), True),
        StructField("status", StringType(), False),
        StructField("started_at", TimestampType(), False),
        StructField("completed_at", TimestampType(), True),
        StructField("source_rows_available", LongType(), True),
        StructField("selected_batch_rows", LongType(), True),
        StructField("rows_before_window", LongType(), True),
        StructField("rows_in_window", LongType(), True),
        StructField("rows_before_dedup", LongType(), True),
        StructField("rows_after_dedup", LongType(), True),
        StructField("rows_written", LongType(), True),
        StructField("duplicates_removed", LongType(), True),
        StructField("total_quantity", DoubleType(), True),
        StructField("total_value", DoubleType(), True),
        StructField("error_message", StringType(), True),
    ])

    empty_df = spark.createDataFrame([], audit_schema)

    (
        empty_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(AUDIT_TABLE)
    )


def audit_insert(
    source_run_id,
    source_window_key,
    window_start,
    window_end_exclusive,
    status,
    started_at,
    completed_at=None,
    source_rows_available=None,
    selected_batch_rows=None,
    rows_before_window=None,
    rows_in_window=None,
    rows_before_dedup=None,
    rows_after_dedup=None,
    rows_written=None,
    duplicates_removed=None,
    total_quantity=None,
    total_value=None,
    error_message=None,
):
    """Append one audit record."""
    row = [(
        PIPELINE_RUN_ID,
        PIPELINE_NAME,
        source_run_id,
        source_window_key,
        window_start,
        window_end_exclusive,
        status,
        started_at,
        completed_at,
        source_rows_available,
        selected_batch_rows,
        rows_before_window,
        rows_in_window,
        rows_before_dedup,
        rows_after_dedup,
        rows_written,
        duplicates_removed,
        total_quantity,
        total_value,
        error_message,
    )]

    audit_schema = StructType([
        StructField("pipeline_run_id", StringType(), False),
        StructField("pipeline_name", StringType(), False),
        StructField("source_run_id", StringType(), True),
        StructField("source_window_key", StringType(), True),
        StructField("window_start", DateType(), True),
        StructField("window_end_exclusive", DateType(), True),
        StructField("status", StringType(), False),
        StructField("started_at", TimestampType(), False),
        StructField("completed_at", TimestampType(), True),
        StructField("source_rows_available", LongType(), True),
        StructField("selected_batch_rows", LongType(), True),
        StructField("rows_before_window", LongType(), True),
        StructField("rows_in_window", LongType(), True),
        StructField("rows_before_dedup", LongType(), True),
        StructField("rows_after_dedup", LongType(), True),
        StructField("rows_written", LongType(), True),
        StructField("duplicates_removed", LongType(), True),
        StructField("total_quantity", DoubleType(), True),
        StructField("total_value", DoubleType(), True),
        StructField("error_message", StringType(), True),
    ])

    (
        spark.createDataFrame(row, audit_schema)
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(AUDIT_TABLE)
    )


def audit_update(status, completed_at=None, error_message=None):
    """Update the current execution status in the audit table."""
    assignments = [f"status = '{sql_escape(status)}'"]

    if completed_at is not None:
        assignments.append(
            f"completed_at = TIMESTAMP '{completed_at.strftime('%Y-%m-%d %H:%M:%S')}'"
        )

    if error_message is not None:
        assignments.append(
            f"error_message = '{sql_escape(error_message[:4000])}'"
        )

    spark.sql(
        f"""
        UPDATE {AUDIT_TABLE}
        SET {", ".join(assignments)}
        WHERE pipeline_run_id = '{sql_escape(PIPELINE_RUN_ID)}'
        """
    )


def ensure_lineage_columns():
    """Add only missing lineage columns to an existing Gold table."""
    expected_types = {
        "source_run_id": "STRING",
        "source_execution_date": "DATE",
        "source_window_start": "DATE",
        "source_window_end": "DATE",
        "source_window_key": "STRING",
        "gold_ingested_at": "TIMESTAMP",
    }

    existing_columns = set(spark.table(TARGET_TABLE).columns)

    if "source_date" not in existing_columns:
        raise RuntimeError(
            f"Existing target {TARGET_TABLE} does not contain source_date. "
            "The table is incompatible with the required partition schema."
        )

    for column_name, data_type in expected_types.items():
        if column_name not in existing_columns:
            spark.sql(
                f"ALTER TABLE {TARGET_TABLE} "
                f"ADD COLUMNS ({column_name} {data_type})"
            )


def validate_no_active_same_scope(source_window_key):
    """Reject a concurrent RUNNING execution for the same logical scope."""
    active_count = (
        spark.table(AUDIT_TABLE)
        .filter(
            (col("pipeline_name") == lit(PIPELINE_NAME))
            & (col("source_window_key") == lit(source_window_key))
            & (col("status") == lit("RUNNING"))
            & (col("pipeline_run_id") != lit(PIPELINE_RUN_ID))
        )
        .count()
    )

    if active_count > 0:
        raise RuntimeError(
            f"Another RUNNING pipeline execution exists for "
            f"source_window_key={source_window_key}."
        )


def validate_required_columns(actual_columns, required_columns, label):
    missing = [c for c in required_columns if c not in actual_columns]
    if missing:
        raise RuntimeError(f"{label} is missing required columns: {missing}")


# =============================================================================
# 3. STARTUP
# =============================================================================

started_at = datetime.now(timezone.utc)
ensure_audit_table()

print("=" * 110)
print("PRODUCTION GOLD RTV PIPELINE")
print("=" * 110)
print(f"Pipeline run ID : {PIPELINE_RUN_ID}")
print(f"Source table    : {SOURCE_TABLE}")
print(f"Target table    : {TARGET_TABLE}")
print(f"Audit table     : {AUDIT_TABLE}")
print("=" * 110)


# Runtime values used by the failure handler.
SOURCE_RUN_ID = None
SOURCE_WINDOW_KEY = None
WINDOW_START = None
WINDOW_END_EXCLUSIVE = None
source_rows_available = None
selected_batch_rows = None
rows_before_window = None
rows_in_window = None
rows_before_dedup = None
rows_after_dedup = None
duplicates_removed = None
written_count = None
total_qty = None
total_value = None


try:
    # =========================================================================
    # 4. VALIDATE SOURCE
    # =========================================================================

    if not table_exists(SOURCE_TABLE):
        raise RuntimeError(f"Source table does not exist: {SOURCE_TABLE}")

    silver = spark.table(SOURCE_TABLE)
    silver_columns = silver.columns
    source_rows_available = silver.count()

    if source_rows_available == 0:
        raise RuntimeError("Silver table contains no rows.")

    validate_required_columns(
        silver_columns,
        REQUIRED_SILVER_COLUMNS,
        "Silver data"
    )
    validate_required_columns(
        silver_columns,
        REQUIRED_METADATA_COLUMNS,
        "Silver metadata"
    )

    print(f"Silver rows available: {source_rows_available:,}")

    # =========================================================================
    # 5. NORMALIZE AND VALIDATE METADATA
    # =========================================================================

    metadata_select = [
        "_run_id",
        "execution_date",
        "from_date",
        "to_date",
    ]

    for optional_column in ("ingested_at", "ingestion_mode", "week_start", "week_end"):
        if optional_column in silver_columns:
            metadata_select.append(optional_column)

    silver_metadata = (
        silver.select(*metadata_select)
        .withColumn("_run_id", trim(col("_run_id").cast("string")))
        .withColumn("_metadata_execution_date", parse_date_column("execution_date"))
        .withColumn("_metadata_from_date", parse_date_column("from_date"))
        .withColumn("_metadata_to_date", parse_date_column("to_date"))
    )

    invalid_metadata_count = (
        silver_metadata
        .filter(
            col("_run_id").isNull()
            | (trim(col("_run_id")) == "")
            | col("_metadata_execution_date").isNull()
            | col("_metadata_from_date").isNull()
            | col("_metadata_to_date").isNull()
            | (col("_metadata_from_date") >= col("_metadata_to_date"))
        )
        .count()
    )

    if invalid_metadata_count > 0:
        raise RuntimeError(
            f"Found {invalid_metadata_count:,} rows with invalid ingestion metadata."
        )

    # =========================================================================
    # 6. SELECT LATEST VALID BATCH
    # =========================================================================

    order_columns = [col("_metadata_execution_date").desc()]

    if "ingested_at" in silver_columns:
        order_columns.append(col("ingested_at").desc_nulls_last())

    order_columns.append(col("_run_id").desc())

    latest_batch = (
        silver_metadata
        .select(*metadata_select, "_metadata_execution_date",
                "_metadata_from_date", "_metadata_to_date")
        .dropDuplicates([
            "_run_id",
            "_metadata_execution_date",
            "_metadata_from_date",
            "_metadata_to_date",
        ])
        .orderBy(*order_columns)
        .limit(1)
        .collect()
    )

    if not latest_batch:
        raise RuntimeError("No valid Silver ingestion batch was found.")

    batch = latest_batch[0].asDict()

    SOURCE_RUN_ID = str(batch["_run_id"]).strip()
    EXECUTION_DATE = parse_python_date(
        batch["_metadata_execution_date"], "execution_date"
    )
    WINDOW_START = parse_python_date(
        batch["_metadata_from_date"], "from_date"
    )
    WINDOW_END_EXCLUSIVE = parse_python_date(
        batch["_metadata_to_date"], "to_date"
    )
    WINDOW_END_INCLUSIVE = WINDOW_END_EXCLUSIVE - timedelta(days=1)

    if WINDOW_START >= WINDOW_END_EXCLUSIVE:
        raise RuntimeError("Invalid processing window.")

    SOURCE_WINDOW_KEY = (
        f"{WINDOW_START:%Y-%m-%d}__{WINDOW_END_EXCLUSIVE:%Y-%m-%d}"
    )

    validate_no_active_same_scope(SOURCE_WINDOW_KEY)

    audit_insert(
        source_run_id=SOURCE_RUN_ID,
        source_window_key=SOURCE_WINDOW_KEY,
        window_start=WINDOW_START,
        window_end_exclusive=WINDOW_END_EXCLUSIVE,
        status="RUNNING",
        started_at=started_at,
    )

    print(f"Selected source run ID : {SOURCE_RUN_ID}")
    print(f"Execution date         : {EXECUTION_DATE}")
    print(f"Window start           : {WINDOW_START}")
    print(f"Window end exclusive   : {WINDOW_END_EXCLUSIVE}")
    print(f"Window key             : {SOURCE_WINDOW_KEY}")

    # =========================================================================
    # 7. SELECT THE CHOSEN BATCH
    # =========================================================================

    selected_batch_df = silver.filter(
        trim(col("_run_id").cast("string")) == lit(SOURCE_RUN_ID)
    )

    selected_batch_rows = selected_batch_df.count()

    if selected_batch_rows == 0:
        raise RuntimeError("Selected Silver run ID contains no rows.")

    # =========================================================================
    # 8. NORMALIZE RTV DATA
    # =========================================================================

    rtv_df = selected_batch_df.select(*REQUIRED_SILVER_COLUMNS)

    for string_column in (
        "rtvId",
        "time",
        "vendorName",
        "productId",
        "productName",
        "barcode",
        "description",
        "storeName",
        "doneBy",
    ):
        rtv_df = rtv_df.withColumn(
            string_column,
            trim(col(string_column).cast("string"))
        )

    for numeric_column in ("quantity", "price", "totalAmount"):
        rtv_df = rtv_df.withColumn(
            numeric_column,
            col(numeric_column).cast("double")
        )

    rtv_df = rtv_df.withColumn("Date", parse_date_column("Date"))

    # =========================================================================
    # 9. DATA QUALITY VALIDATION
    # =========================================================================

    invalid_date_count = rtv_df.filter(col("Date").isNull()).count()
    if invalid_date_count > 0:
        raise RuntimeError(
            f"{invalid_date_count:,} records have NULL or invalid Date values."
        )

    invalid_store_count = rtv_df.filter(
        col("storeName").isNull() | (trim(col("storeName")) == "")
    ).count()

    if invalid_store_count > 0:
        raise RuntimeError(
            f"{invalid_store_count:,} records have NULL or empty storeName."
        )

    invalid_quantity_count = rtv_df.filter(col("quantity").isNull()).count()
    invalid_amount_count = rtv_df.filter(col("totalAmount").isNull()).count()

    if invalid_quantity_count > 0 or invalid_amount_count > 0:
        raise RuntimeError(
            "Null quantity or totalAmount values detected. "
            f"null_quantity={invalid_quantity_count:,}, "
            f"null_totalAmount={invalid_amount_count:,}."
        )

    rows_before_window = rtv_df.count()

    rtv_df = rtv_df.filter(
        (col("Date") >= lit(WINDOW_START))
        & (col("Date") < lit(WINDOW_END_EXCLUSIVE))
    )

    rows_in_window = rtv_df.count()

    if rows_in_window == 0:
        raise RuntimeError("No RTV records exist inside the selected window.")

    # =========================================================================
    # 10. ADD LINEAGE AND PARTITION COLUMNS
    # =========================================================================

    rtv_df = (
        rtv_df
        .withColumn("source_date", lit(WINDOW_END_INCLUSIVE).cast("date"))
        .withColumn("source_run_id", lit(SOURCE_RUN_ID).cast("string"))
        .withColumn("source_execution_date", lit(EXECUTION_DATE).cast("date"))
        .withColumn("source_window_start", lit(WINDOW_START).cast("date"))
        .withColumn("source_window_end", lit(WINDOW_END_EXCLUSIVE).cast("date"))
        .withColumn("source_window_key", lit(SOURCE_WINDOW_KEY).cast("string"))
        .withColumn("gold_ingested_at", F.current_timestamp())
    )

    # =========================================================================
    # 11. DEDUPLICATION
    # =========================================================================

    rows_before_dedup = rtv_df.count()

    rtv_df = rtv_df.dropDuplicates(DEDUPLICATION_COLUMNS)

    rows_after_dedup = rtv_df.count()
    duplicates_removed = rows_before_dedup - rows_after_dedup

    if rows_after_dedup == 0:
        raise RuntimeError("No records remain after deduplication.")

    # =========================================================================
    # 12. FINAL PRE-WRITE VALIDATION
    # =========================================================================

    invalid_lineage_count = rtv_df.filter(
        col("source_window_key").isNull()
        | (col("source_window_key") != lit(SOURCE_WINDOW_KEY))
        | (col("source_date") != lit(WINDOW_END_INCLUSIVE))
    ).count()

    if invalid_lineage_count > 0:
        raise RuntimeError(
            f"{invalid_lineage_count:,} records failed lineage validation."
        )

    outside_final_count = rtv_df.filter(
        (col("Date") < lit(WINDOW_START))
        | (col("Date") >= lit(WINDOW_END_EXCLUSIVE))
    ).count()

    if outside_final_count > 0:
        raise RuntimeError(
            f"{outside_final_count:,} records fall outside the processing window."
        )

    # =========================================================================
    # 13. PREPARE TARGET
    # =========================================================================

    target_exists = table_exists(TARGET_TABLE)

    if target_exists:
        ensure_lineage_columns()

        target_columns = set(spark.table(TARGET_TABLE).columns)
        if "source_date" not in target_columns:
            raise RuntimeError(
                "Target table does not contain source_date. "
                "Existing partition compatibility cannot be guaranteed."
            )

        # No broad source_date fallback is used here.
        # Only the exact logical scope is replaced.
        safe_window_key = sql_escape(SOURCE_WINDOW_KEY)

        existing_scope_count = (
            spark.table(TARGET_TABLE)
            .filter(col("source_window_key") == lit(SOURCE_WINDOW_KEY))
            .count()
        )

        print(f"Existing rows for exact scope: {existing_scope_count:,}")

        spark.sql(
            f"""
            DELETE FROM {TARGET_TABLE}
            WHERE source_window_key = '{safe_window_key}'
            """
        )

    # =========================================================================
    # 14. WRITE GOLD
    # =========================================================================

    if target_exists:
        (
            rtv_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "false")
            .saveAsTable(TARGET_TABLE)
        )
    else:
        (
            rtv_df.write
            .format("delta")
            .mode("append")
            .partitionBy("source_date")
            .option("mergeSchema", "false")
            .saveAsTable(TARGET_TABLE)
        )

    # =========================================================================
    # 15. POST-WRITE VALIDATION
    # =========================================================================

    written_df = (
        spark.table(TARGET_TABLE)
        .filter(col("source_window_key") == lit(SOURCE_WINDOW_KEY))
    )

    written_count = written_df.count()

    if written_count != rows_after_dedup:
        raise RuntimeError(
            "Post-write count mismatch. "
            f"expected={rows_after_dedup:,}, actual={written_count:,}."
        )

    validation_conditions = [
        col("source_run_id") != lit(SOURCE_RUN_ID),
        col("source_window_start") != lit(WINDOW_START),
        col("source_window_end") != lit(WINDOW_END_EXCLUSIVE),
        col("source_date") != lit(WINDOW_END_INCLUSIVE),
        (col("Date") < lit(WINDOW_START)),
        (col("Date") >= lit(WINDOW_END_EXCLUSIVE)),
    ]

    invalid_written_condition = validation_conditions[0]
    for validation_condition in validation_conditions[1:]:
        invalid_written_condition = invalid_written_condition | validation_condition

    invalid_written_count = written_df.filter(
        invalid_written_condition
    ).count()

    if invalid_written_count > 0:
        raise RuntimeError(
            f"{invalid_written_count:,} written records failed post-write validation."
        )

    duplicate_group_count = (
        written_df
        .groupBy(*DEDUPLICATION_COLUMNS)
        .count()
        .filter(col("count") > 1)
        .count()
    )

    if duplicate_group_count > 0:
        raise RuntimeError(
            f"{duplicate_group_count:,} duplicate groups detected in Gold scope."
        )

    summary = written_df.agg(
        spark_sum("quantity").alias("total_qty"),
        spark_sum("totalAmount").alias("total_value"),
    ).collect()[0]

    total_qty = float(summary["total_qty"] or 0.0)
    total_value = float(summary["total_value"] or 0.0)

    # =========================================================================
    # 16. SUCCESS AUDIT
    # =========================================================================

    completed_at = datetime.now(timezone.utc)

    audit_update(status="SUCCESS", completed_at=completed_at)


    print("=" * 110)
    print("FINAL GOLD RTV VALIDATION")
    print("=" * 110)
    print(f"Pipeline run ID       : {PIPELINE_RUN_ID}")
    print(f"Source run ID         : {SOURCE_RUN_ID}")
    print(f"Source window key     : {SOURCE_WINDOW_KEY}")
    print(f"Silver rows available : {source_rows_available:,}")
    print(f"Selected batch rows   : {selected_batch_rows:,}")
    print(f"Rows before window    : {rows_before_window:,}")
    print(f"Rows inside window    : {rows_in_window:,}")
    print(f"Rows before dedup     : {rows_before_dedup:,}")
    print(f"Rows after dedup      : {rows_after_dedup:,}")
    print(f"Duplicates removed    : {duplicates_removed:,}")
    print(f"Gold rows written     : {written_count:,}")
    print(f"Total quantity        : {total_qty:,.2f}")
    print(f"Total RTV value       : ₹{total_value:,.2f}")
    print(f"Duplicate groups      : {duplicate_group_count:,}")
    print("=" * 110)
    print("SUCCESS: GOLD RTV PIPELINE COMPLETED")
    print("=" * 110)


except Exception as exc:
    error_message = str(exc)[:4000]

    print("=" * 110)
    print("GOLD RTV PIPELINE FAILED")
    print("=" * 110)
    print(error_message)
    print("=" * 110)

    # Failure audit is best effort. Do not hide the original exception.
    try:
        audit_update(
            status="FAILED",
            completed_at=datetime.now(timezone.utc),
            error_message=error_message,
        )

    except Exception as audit_exc:
        print(f"WARNING: Failed to write failure audit: {audit_exc}")

    raise


# COMMAND ----------

