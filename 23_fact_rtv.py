# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# ============================================================
# PRODUCTION FACT RTV PIPELINE
# Idempotent | Incremental | Dynamic | Delta Lake
# ============================================================

from datetime import datetime, timezone, timedelta
from functools import reduce
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit
from delta.tables import DeltaTable

# ============================================================
# CONFIGURATION
# ============================================================

RTV_SOURCE_TABLE = f"{catalog_name}.{schema_name}.rtv_silver"
RTV_TARGET_TABLE = f"{catalog_name}.{schema_name}.fact_rtv"
RTV_AUDIT_TABLE = f"{catalog_name}.{schema_name}.fact_rtv_pipeline_audit"

RTV_PRODUCT_DIM = f"{catalog_name}.{schema_name}.dim_product"
RTV_STORE_DIM = f"{catalog_name}.{schema_name}.dim_store"
RTV_DATE_DIM = f"{catalog_name}.{schema_name}.dim_date"

PIPELINE_RUN_ID = str(uuid4())
STARTED_AT = datetime.now(timezone.utc)

print("=" * 110)
print("PRODUCTION FACT RTV PIPELINE")
print("=" * 110)
print(f"Pipeline run ID : {PIPELINE_RUN_ID}")
print(f"Source table    : {RTV_SOURCE_TABLE}")
print(f"Target table    : {RTV_TARGET_TABLE}")
print(f"Audit table     : {RTV_AUDIT_TABLE}")
print("=" * 110)

# ============================================================
# HELPERS
# ============================================================

def table_exists(table_name):
    return spark.catalog.tableExists(table_name)


def normalize_store_name(expression):
    return F.lower(
        F.trim(
            F.regexp_replace(
                F.regexp_replace(expression.cast("string"), r"[\t\r\n\u00A0]+", " "),
                r"\s+",
                " ",
            )
        )
    )


def normalize_barcode(expression):
    return F.trim(
        F.regexp_replace(
            expression.cast("string"),
            r"\.0+$",
            "",
        )
    )


def parse_rtv_date(expression):
    """
    Safely parse RTV date values.

    Supported:
      - DATE
      - TIMESTAMP
      - yyyy-MM-dd
      - yyyy/MM/dd
      - M/d/yyyy
      - timestamp-like strings beginning with those formats
    """

    value = F.trim(expression.cast("string"))

    return (
        F.when(
            value.rlike(r"^\d{4}-\d{2}-\d{2}(?:$|[ T].*)"),
            F.to_date(
                F.substring(value, 1, 10),
                "yyyy-MM-dd"
            )
        )
        .when(
            value.rlike(r"^\d{4}/\d{1,2}/\d{1,2}(?:$|[ T].*)"),
            F.to_date(
                F.regexp_extract(
                    value,
                    r"^(\d{4}/\d{1,2}/\d{1,2})",
                    1
                ),
                "yyyy/M/d"
            )
        )
        .when(
            value.rlike(r"^\d{1,2}/\d{1,2}/\d{4}(?:$|[ T].*)"),
            F.to_date(
                F.regexp_extract(
                    value,
                    r"^(\d{1,2}/\d{1,2}/\d{4})",
                    1
                ),
                "M/d/yyyy"
            )
        )
        .otherwise(F.lit(None).cast("date"))
    )

def sql_quote(value):
    return str(value).replace("'", "''")


def ensure_audit_table():
    if not table_exists(RTV_AUDIT_TABLE):
        spark.sql(
            f"""
            CREATE TABLE {RTV_AUDIT_TABLE} (
                pipeline_run_id STRING,
                source_run_id STRING,
                source_window_key STRING,
                window_start DATE,
                window_end_exclusive DATE,
                status STRING,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                source_rows BIGINT,
                accepted_rows BIGINT,
                fact_rows BIGINT,
                total_quantity DOUBLE,
                total_amount DOUBLE,
                error_message STRING
            ) USING DELTA
            """
        )


def write_audit(status, source_run_id=None, window_key=None, window_start=None,
                window_end_exclusive=None, source_rows=None, accepted_rows=None,
                fact_rows=None, total_quantity=None, total_amount=None,
                error_message=None):
    ensure_audit_table()
    completed_at = datetime.now(timezone.utc) if status in ("SUCCESS", "FAILED") else None
    audit_row = spark.createDataFrame(
        [(
            PIPELINE_RUN_ID,
            source_run_id,
            window_key,
            window_start,
            window_end_exclusive,
            status,
            STARTED_AT,
            completed_at,
            source_rows,
            accepted_rows,
            fact_rows,
            float(total_quantity) if total_quantity is not None else None,
            float(total_amount) if total_amount is not None else None,
            error_message,
        )],
        "pipeline_run_id string, source_run_id string, source_window_key string, "
        "window_start date, window_end_exclusive date, status string, "
        "started_at timestamp, completed_at timestamp, source_rows long, "
        "accepted_rows long, fact_rows long, total_quantity double, "
        "total_amount double, error_message string",
    )
    audit_row.write.format("delta").mode("append").saveAsTable(RTV_AUDIT_TABLE)


def add_missing_target_columns():
    if not table_exists(RTV_TARGET_TABLE):
        return

    target_columns = set(spark.table(RTV_TARGET_TABLE).columns)
    required_lineage = {
        "source_run_id": "STRING",
        "source_execution_date": "DATE",
        "source_window_start": "DATE",
        "source_window_end_exclusive": "DATE",
        "source_window_key": "STRING",
        "gold_ingested_at": "TIMESTAMP",
    }

    for column_name, data_type in required_lineage.items():
        if column_name not in target_columns:
            spark.sql(
                f"ALTER TABLE {RTV_TARGET_TABLE} ADD COLUMNS ({column_name} {data_type})"
            )


def validate_required_columns(dataframe, required_columns, dataset_name):
    missing = [c for c in required_columns if c not in dataframe.columns]
    if missing:
        raise RuntimeError(f"Missing required {dataset_name} columns: {missing}")


# ============================================================
# MAIN PIPELINE
# ============================================================

source_run_id = None
source_window_key = None
window_start = None
window_end_exclusive = None
raw_source_count = 0
accepted_source_count = 0
fact_row_count = 0

audit_written = False

try:
    # --------------------------------------------------------
    # 1. SOURCE TABLE VALIDATION
    # --------------------------------------------------------
    for required_table in [RTV_SOURCE_TABLE, RTV_PRODUCT_DIM, RTV_STORE_DIM, RTV_DATE_DIM]:
        if not table_exists(required_table):
            raise RuntimeError(f"Required table does not exist: {required_table}")

    rtv = spark.table(RTV_SOURCE_TABLE)
    raw_source_count = rtv.count()

    required_source_columns = [
        "rtvId", "Date", "time", "vendorName", "productId", "productName",
        "barcode", "quantity", "price", "totalAmount", "description",
        "storeName", "doneBy",
    ]
    validate_required_columns(rtv, required_source_columns, "RTV source")

    if raw_source_count == 0:
        raise RuntimeError("RTV source contains zero rows; no fact data generated.")

    # --------------------------------------------------------
    # 2. ROBUST SOURCE BATCH / WINDOW RESOLUTION
    # --------------------------------------------------------
    # Silver schemas can differ between ingestion versions. Resolve
    # metadata from supported column aliases when available. If the
    # source has no batch metadata, process the complete available
    # source date range instead of failing with a false metadata error.
    available_columns = set(rtv.columns)

    def first_existing_column(candidates):
        return next((name for name in candidates if name in available_columns), None)

    run_id_column = first_existing_column([
        "_run_id", "source_run_id", "_source_run_id", "run_id"
    ])
    execution_date_column = first_existing_column([
        "execution_date", "_metadata_execution_date", "source_execution_date",
        "_source_execution_date"
    ])
    from_date_column = first_existing_column([
        "from_date", "_metadata_from_date", "source_window_start",
        "_source_window_start", "window_start"
    ])
    to_date_column = first_existing_column([
        "to_date", "_metadata_to_date", "source_window_end_exclusive",
        "_source_window_end_exclusive", "window_end_exclusive"
    ])

    def optional_date_column(column_name):
        if column_name:
            return parse_rtv_date(col(column_name))
        return F.lit(None).cast("date")

    if run_id_column and execution_date_column and from_date_column and to_date_column:
        batch_metadata = (
            rtv
            .select(
                F.trim(col(run_id_column).cast("string")).alias("source_run_id"),
                optional_date_column(execution_date_column).alias("source_execution_date"),
                optional_date_column(from_date_column).alias("window_start"),
                optional_date_column(to_date_column).alias("window_end_exclusive"),
            )
            .filter(
                col("source_run_id").isNotNull()
                & (col("source_run_id") != "")
                & col("source_execution_date").isNotNull()
                & col("window_start").isNotNull()
                & col("window_end_exclusive").isNotNull()
                & (col("window_end_exclusive") > col("window_start"))
            )
            .dropDuplicates()
            .orderBy(
                col("source_execution_date").desc(),
                col("window_end_exclusive").desc(),
                col("window_start").desc(),
                col("source_run_id").desc(),
            )
        )
        selected_metadata = batch_metadata.first()
    else:
        selected_metadata = None

    if selected_metadata is None:
        # Fallback for Silver tables without batch metadata.
        # The entire available Date range becomes one deterministic
        # processing scope. This is safe for replaceWhere because the
        # target replacement is restricted to represented RTV dates.
        source_dates = (
            rtv
            .select(parse_rtv_date(col("Date")).alias("_source_date"))
            .filter(col("_source_date").isNotNull())
            .agg(
                F.min("_source_date").alias("window_start"),
                F.max("_source_date").alias("max_source_date"),
            )
            .first()
        )

        if source_dates is None or source_dates["window_start"] is None:
            raise RuntimeError(
                "No valid RTV dates found in the source table. "
                "Cannot derive a processing window."
            )

        window_start = source_dates["window_start"]
        max_source_date = source_dates["max_source_date"]
        window_end_exclusive = max_source_date + timedelta(days=1)
        source_execution_date = window_end_exclusive
        source_run_id = f"silver_full_{window_start}_{max_source_date}"
        source_window_key = f"{window_start}__{window_end_exclusive}"
        use_run_id_filter = False

        print("Source batch metadata not available; using complete source date range.")
    else:
        source_run_id = selected_metadata["source_run_id"]
        source_execution_date = selected_metadata["source_execution_date"]
        window_start = selected_metadata["window_start"]
        window_end_exclusive = selected_metadata["window_end_exclusive"]
        source_window_key = f"{window_start}__{window_end_exclusive}"
        use_run_id_filter = True

    print(f"Selected source run ID : {source_run_id}")
    print(f"Execution date         : {source_execution_date}")
    print(f"Window start           : {window_start}")
    print(f"Window end exclusive   : {window_end_exclusive}")
    print(f"Window key             : {source_window_key}")

    # --------------------------------------------------------
    # 3. STRICT SOURCE BATCH / WINDOW FILTER
    # --------------------------------------------------------
    source_batch = rtv

    if use_run_id_filter:
        source_batch = source_batch.filter(
            F.trim(col(run_id_column).cast("string")) == lit(source_run_id)
        )

    source_batch = (
        source_batch
        .withColumn("_rtv_date", parse_rtv_date(col("Date")))
        .withColumn("_barcode", normalize_barcode(col("barcode")))
        .withColumn("_product_id", F.trim(col("productId").cast("string")))
        .withColumn("_product_name", F.trim(col("productName").cast("string")))
        .withColumn("_store_name", F.trim(col("storeName").cast("string")))
        .withColumn("_normalized_store_name", normalize_store_name(col("storeName")))
        .withColumn("_vendor_name", F.trim(col("vendorName").cast("string")))
        .withColumn("_quantity", col("quantity").cast("double"))
        .withColumn("_price", col("price").cast("double"))
        .withColumn("_total_amount", col("totalAmount").cast("double"))
        .withColumn("_rtv_id", F.trim(col("rtvId").cast("string")))
        .withColumn("_time", col("time").cast("string"))
        .withColumn("_description", col("description").cast("string"))
        .withColumn("_done_by", col("doneBy").cast("string"))
    )

    selected_batch_count = source_batch.count()
    if selected_batch_count == 0:
        raise RuntimeError(f"Selected source scope has zero rows: {source_run_id}")

    invalid_source = source_batch.filter(
        col("_rtv_date").isNull()
        | (col("_rtv_date") < lit(window_start))
        | (col("_rtv_date") >= lit(window_end_exclusive))
        | col("_barcode").isNull()
        | (col("_barcode") == "")
        | col("_normalized_store_name").isNull()
        | (col("_normalized_store_name") == "")
        | col("_quantity").isNull()
        | col("_total_amount").isNull()
    )
    invalid_source_count = invalid_source.count()
    if invalid_source_count > 0:
        raise RuntimeError(f"Invalid source rows in selected scope: {invalid_source_count:,}")

    source_batch = source_batch.filter(
        (col("_rtv_date") >= lit(window_start))
        & (col("_rtv_date") < lit(window_end_exclusive))
    )

    print(f"Selected source run ID : {source_run_id}")
    print(f"Execution date         : {source_execution_date}")
    print(f"Window start           : {window_start}")
    print(f"Window end exclusive   : {window_end_exclusive}")
    print(f"Window key             : {source_window_key}")

    # --------------------------------------------------------
    # 3. STRICT SOURCE BATCH FILTER
    # --------------------------------------------------------
    source_batch = (
        rtv
        .filter(F.trim(col("_run_id").cast("string")) == lit(source_run_id))
        .withColumn("_rtv_date", parse_rtv_date(col("Date")))
        .withColumn("_barcode", normalize_barcode(col("barcode")))
        .withColumn("_product_id", F.trim(col("productId").cast("string")))
        .withColumn("_product_name", F.trim(col("productName").cast("string")))
        .withColumn("_store_name", F.trim(col("storeName").cast("string")))
        .withColumn("_normalized_store_name", normalize_store_name(col("storeName")))
        .withColumn("_vendor_name", F.trim(col("vendorName").cast("string")))
        .withColumn("_quantity", col("quantity").cast("double"))
        .withColumn("_price", col("price").cast("double"))
        .withColumn("_total_amount", col("totalAmount").cast("double"))
        .withColumn("_rtv_id", F.trim(col("rtvId").cast("string")))
        .withColumn("_time", col("time").cast("string"))
        .withColumn("_description", col("description").cast("string"))
        .withColumn("_done_by", col("doneBy").cast("string"))
    )

    selected_batch_count = source_batch.count()
    if selected_batch_count == 0:
        raise RuntimeError(f"Selected source run has zero rows: {source_run_id}")

    invalid_source = source_batch.filter(
        col("_rtv_date").isNull()
        | (col("_rtv_date") < lit(window_start))
        | (col("_rtv_date") >= lit(window_end_exclusive))
        | col("_barcode").isNull()
        | (col("_barcode") == "")
        | col("_normalized_store_name").isNull()
        | (col("_normalized_store_name") == "")
        | col("_quantity").isNull()
        | col("_total_amount").isNull()
    )
    invalid_source_count = invalid_source.count()
    if invalid_source_count > 0:
        raise RuntimeError(f"Invalid source rows in selected batch: {invalid_source_count:,}")

    source_batch = source_batch.filter(
        (col("_rtv_date") >= lit(window_start))
        & (col("_rtv_date") < lit(window_end_exclusive))
    )

    # --------------------------------------------------------
    # 4. DIMENSION VALIDATION AND LOOKUPS
    # --------------------------------------------------------
    dim_product = spark.table(RTV_PRODUCT_DIM)
    dim_store = spark.table(RTV_STORE_DIM)
    dim_date = spark.table(RTV_DATE_DIM)

    product_lookup = (
        dim_product
        .select(
            normalize_barcode(col("barcode")).alias("_dim_barcode"),
            col("product_key"),
            col("product_id").alias("dim_product_id"),
            col("product_name").alias("dim_product_name"),
            col("brand_name").alias("dim_brand_name"),
            col("category_name").alias("dim_category_name"),
            col("hsn_code").alias("dim_hsn_code"),
        )
        .filter(col("_dim_barcode").isNotNull() & (col("_dim_barcode") != ""))
        .dropDuplicates(["_dim_barcode"])
    )

    store_lookup = (
        dim_store
        .select(
            col("store_key"),
            col("store_id").alias("dim_store_id"),
            col("store_name").alias("dim_store_name"),
            col("normalized_store_name"),
        )
        .filter(col("normalized_store_name").isNotNull() & (col("normalized_store_name") != ""))
        .dropDuplicates(["normalized_store_name"])
    )

    date_lookup = (
        dim_date
        .select(col("date").alias("_dim_date"), col("date_key"))
        .dropDuplicates(["_dim_date"])
    )

    accepted_rtv = source_batch.join(
        store_lookup.select("normalized_store_name"),
        source_batch["_normalized_store_name"] == store_lookup["normalized_store_name"],
        "left_semi",
    )
    accepted_source_count = accepted_rtv.count()

    excluded_source_count = selected_batch_count - accepted_source_count
    if excluded_source_count > 0:
        excluded_stores = (
            source_batch
            .join(
                store_lookup.select("normalized_store_name"),
                source_batch["_normalized_store_name"] == store_lookup["normalized_store_name"],
                "left_anti",
            )
            .groupBy("_store_name", "_normalized_store_name")
            .count()
            .orderBy(F.desc("count"))
        )
        excluded_stores.show(20, truncate=False)
        raise RuntimeError(f"{excluded_source_count:,} source rows belong to stores absent from dim_store.")

    rtv_product = accepted_rtv.join(
        product_lookup,
        accepted_rtv["_barcode"] == product_lookup["_dim_barcode"],
        "left",
    )
    unmatched_product_count = rtv_product.filter(col("product_key").isNull()).count()
    if unmatched_product_count > 0:
        sample = [r["_barcode"] for r in rtv_product.filter(col("product_key").isNull()).select("_barcode").distinct().limit(20).collect()]
        raise RuntimeError(f"Unmatched product barcodes: {unmatched_product_count:,}; sample={sample}")

    rtv_store = rtv_product.join(
        store_lookup,
        rtv_product["_normalized_store_name"] == store_lookup["normalized_store_name"],
        "left",
    )
    unmatched_store_count = rtv_store.filter(col("store_key").isNull()).count()
    if unmatched_store_count > 0:
        raise RuntimeError(f"Unmatched store rows after lookup: {unmatched_store_count:,}")

    rtv_date = rtv_store.join(
        date_lookup,
        rtv_store["_rtv_date"] == date_lookup["_dim_date"],
        "left",
    )
    unmatched_date_count = rtv_date.filter(col("date_key").isNull()).count()
    if unmatched_date_count > 0:
        raise RuntimeError(f"Unmatched dates in dim_date: {unmatched_date_count:,}")

    # --------------------------------------------------------
    # 5. FACT AGGREGATION
    # --------------------------------------------------------
    fact_rtv = (
        rtv_date
        .groupBy("product_key", "store_key", "date_key", "_rtv_date")
        .agg(
            F.first("_barcode", ignorenulls=True).alias("barcode"),
            F.first("_product_id", ignorenulls=True).alias("source_product_id"),
            F.first("_product_name", ignorenulls=True).alias("product_name"),
            F.first("_store_name", ignorenulls=True).alias("source_store_name"),
            F.first("_normalized_store_name", ignorenulls=True).alias("source_normalized_store_name"),
            F.first(F.col("dim_store_id").cast("string"), ignorenulls=True).alias("dim_store_id"),
            F.first("dim_store_name", ignorenulls=True).alias("dim_store_name"),
            F.first("_vendor_name", ignorenulls=True).alias("vendor_name"),
            F.first("_rtv_id", ignorenulls=True).alias("rtv_id"),
            F.first("_time", ignorenulls=True).alias("rtv_time"),
            F.first("_description", ignorenulls=True).alias("description"),
            F.first("_done_by", ignorenulls=True).alias("done_by"),
            F.sum(F.coalesce(col("_quantity"), lit(0.0))).alias("quantity"),
            F.max("_price").alias("price"),
            F.sum(F.coalesce(col("_total_amount"), lit(0.0))).alias("total_amount"),
        )
        .withColumn("rtv_date", col("_rtv_date"))
        .drop("_rtv_date")
        .withColumn("source_run_id", lit(source_run_id))
        .withColumn("source_execution_date", lit(source_execution_date).cast("date"))
        .withColumn("source_window_start", lit(window_start).cast("date"))
        .withColumn("source_window_end_exclusive", lit(window_end_exclusive).cast("date"))
        .withColumn("source_window_key", lit(source_window_key))
        .withColumn("gold_ingested_at", F.current_timestamp())
    )

    fact_rtv_count = fact_rtv.count()
    duplicate_fact_grain = (
        fact_rtv.groupBy("product_key", "store_key", "date_key")
        .count()
        .filter(col("count") > 1)
        .count()
    )
    if duplicate_fact_grain > 0:
        raise RuntimeError(f"Duplicate fact grain combinations: {duplicate_fact_grain:,}")

    # --------------------------------------------------------
    # 6. TARGET SCHEMA AND ATOMIC INCREMENTAL WRITE
    # --------------------------------------------------------
    if not table_exists(RTV_TARGET_TABLE):
        (
            fact_rtv.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("rtv_date")
            .saveAsTable(RTV_TARGET_TABLE)
        )
    else:
        add_missing_target_columns()

        target_columns = set(spark.table(RTV_TARGET_TABLE).columns)
        missing_write_columns = [c for c in fact_rtv.columns if c not in target_columns]
        if missing_write_columns:
            raise RuntimeError(f"Target table is missing required columns: {missing_write_columns}")

        # Replace only the exact dates present in this selected batch.
        # Delta replaceWhere is atomic and avoids the previous DELETE + APPEND gap.
        represented_dates = [r["rtv_date"] for r in fact_rtv.select("rtv_date").distinct().collect()]
        if not represented_dates:
            raise RuntimeError("No represented RTV dates available for target replacement.")

        date_predicate = " OR ".join(
            [f"rtv_date = DATE '{sql_quote(d)}'" for d in represented_dates]
        )

        (
            fact_rtv.write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere", date_predicate)
            .option("mergeSchema", "false")
            .saveAsTable(RTV_TARGET_TABLE)
        )

    # --------------------------------------------------------
    # 7. POST-WRITE VALIDATION
    # --------------------------------------------------------
    written_rtv = spark.table(RTV_TARGET_TABLE)
    written_scope = written_rtv.filter(
        (col("source_window_key") == lit(source_window_key))
        & (col("source_run_id") == lit(source_run_id))
    )

    written_scope_count = written_scope.count()
    if written_scope_count != fact_rtv_count:
        raise RuntimeError(
            f"Post-write scope count mismatch: expected={fact_rtv_count:,}, actual={written_scope_count:,}"
        )

    invalid_written = written_scope.filter(
        col("product_key").isNull()
        | col("store_key").isNull()
        | col("date_key").isNull()
        | col("rtv_date").isNull()
        | (col("rtv_date") < lit(window_start))
        | (col("rtv_date") >= lit(window_end_exclusive))
    ).count()
    if invalid_written > 0:
        raise RuntimeError(f"Invalid rows found in written scope: {invalid_written:,}")

    written_duplicate_count = (
        written_scope.groupBy("product_key", "store_key", "date_key")
        .count()
        .filter(col("count") > 1)
        .count()
    )
    if written_duplicate_count > 0:
        raise RuntimeError(f"Written fact grain duplicates: {written_duplicate_count:,}")

    summary = fact_rtv.agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("total_amount").alias("total_amount"),
    ).collect()[0]

    total_quantity = float(summary["total_quantity"] or 0.0)
    total_amount = float(summary["total_amount"] or 0.0)

    write_audit(
        status="SUCCESS",
        source_run_id=source_run_id,
        window_key=source_window_key,
        window_start=window_start,
        window_end_exclusive=window_end_exclusive,
        source_rows=selected_batch_count,
        accepted_rows=accepted_source_count,
        fact_rows=fact_rtv_count,
        total_quantity=total_quantity,
        total_amount=total_amount,
    )
    audit_written = True

    print("=" * 110)
    print("FINAL FACT RTV VALIDATION")
    print("=" * 110)
    print(f"Pipeline run ID       : {PIPELINE_RUN_ID}")
    print(f"Source run ID         : {source_run_id}")
    print(f"Source window key     : {source_window_key}")
    print(f"Selected batch rows   : {selected_batch_count:,}")
    print(f"Accepted source rows  : {accepted_source_count:,}")
    print(f"Fact rows generated   : {fact_rtv_count:,}")
    print(f"Fact rows written     : {written_scope_count:,}")
    print(f"Total quantity        : {total_quantity:,.2f}")
    print(f"Total RTV value       : ₹{total_amount:,.2f}")
    print(f"Duplicate grain count : {written_duplicate_count:,}")
    print("=" * 110)
    print("SUCCESS: FACT RTV PIPELINE COMPLETED")
    print("=" * 110)

except Exception as exc:
    error_text = str(exc)[:4000]
    try:
        write_audit(
            status="FAILED",
            source_run_id=source_run_id,
            window_key=source_window_key,
            window_start=window_start,
            window_end_exclusive=window_end_exclusive,
            source_rows=raw_source_count,
            accepted_rows=accepted_source_count,
            fact_rows=fact_row_count,
            error_message=error_text,
        )
    except Exception as audit_error:
        print(f"Audit write failed: {audit_error}")

    print("=" * 110)
    print("FAILED: FACT RTV PIPELINE")
    print(error_text)
    print("=" * 110)
    raise


# COMMAND ----------

