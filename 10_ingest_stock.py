# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# ============================================================
# 10_ingest_stock
# Production-equivalent Stock ingestion → Databricks Bronze
# ============================================================
#
# Production behavior preserved:
#
#   1. Login to The New Shop API
#   2. Fetch CURRENT stock for every store
#   3. Stock API receives ONLY store ID
#   4. snapshot_date is assigned locally
#   5. snapshot_date = TARGET_DATE
#   6. Missing store ID -> skip
#   7. API/fetch failure -> retry
#   8. Any genuine failure -> fail entire task
#   9. No partial Delta commit
#  10. Successful rerun replaces TARGET_DATE snapshot
#
# Databricks improvements:
#
#   - Delta Bronze table
#   - Schema evolution
#   - Atomic/fail-safe write pattern
#   - Post-write validation
#   - Run ID
#   - Store-level failure reporting
#
# ============================================================


# ============================================================
# IMPORTS
# ============================================================

import base64
import io
import time
import uuid
import requests
import pandas as pd

from datetime import datetime, timezone

from pyspark.sql import functions as F


# ============================================================
# CONFIGURATION VALIDATION
# ============================================================

required_config = [
    "API_BASE_URL",
    "API_USERNAME",
    "API_PASSWORD",
    "catalog_name",
    "schema_name",
    "volume_name",
    "TARGET_DATE",
]


missing_config = [
    x
    for x in required_config
    if x not in globals()
    or globals()[x] in (None, "")
]


if missing_config:

    raise RuntimeError(
        f"❌ Missing required configuration values: "
        f"{missing_config}"
    )


# ============================================================
# RUNTIME CONFIGURATION
# ============================================================

STOCK_URL = (
    f"{API_BASE_URL.rstrip('/')}"
    "/store-stocks/report/storeStocksCSV"
)


PARTNER_FILE = (
    f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"
    "/partner.csv"
)


BRONZE_TABLE = (
    f"{catalog_name}.{schema_name}.stock_bronze"
)


MAX_RETRIES = 3

REQUEST_TIMEOUT = 120

RUN_ID = str(uuid.uuid4())


# ============================================================
# STOCK SNAPSHOT DATE
# ============================================================
#
# IMPORTANT:
#
# The stock API does NOT accept a date.
#
# It returns the CURRENT stock for the requested store.
#
# We therefore assign:
#
#     snapshot_date = TARGET_DATE
#
# TARGET_DATE comes from the Databricks job execution date.
#
# Example:
#
# Job execution date = 2026-09-18
#
# API call:
#
#     /store-stocks/report/storeStocksCSV?store=247
#
# Returned data = current stock at API call time
#
# Stored as:
#
#     snapshot_date = 2026-09-18
#
# ============================================================

RUN_DATE = datetime.strptime(
    str(TARGET_DATE),
    "%Y-%m-%d"
).date()


# ============================================================
# START
# ============================================================

print("=" * 70)
print("STOCK INGESTION")
print("=" * 70)

print(f"Run ID       : {RUN_ID}")
print(f"Snapshot date: {RUN_DATE}")
print(f"Bronze table : {BRONZE_TABLE}")

print("=" * 70)


# ============================================================
# LOGIN
# ============================================================

def login():

    """
    Authenticate against The New Shop API.

    Credentials come from 00_config / Databricks secrets.

    Returns:
        token
    """

    login_url = (
        f"{API_BASE_URL.rstrip('/')}/login"
    )

    payload = {
        "username": API_USERNAME,
        "password": API_PASSWORD
    }

    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            response = requests.post(
                login_url,
                json=payload,
                timeout=REQUEST_TIMEOUT
            )

            # ------------------------------------------------
            # Production behavior:
            # login response is parsed and token presence
            # is checked.
            # ------------------------------------------------

            try:

                data = response.json()

            except Exception as exc:

                raise RuntimeError(
                    "Login API returned a non-JSON response."
                ) from exc


            token = data.get("token")


            if not token:

                raise RuntimeError(
                    f"Login failed. "
                    f"HTTP {response.status_code}: "
                    f"{data}"
                )


            return token


        except Exception as exc:

            last_error = exc

            print(
                f"❌ Login attempt "
                f"{attempt}/{MAX_RETRIES} failed: "
                f"{exc}"
            )

            if attempt < MAX_RETRIES:

                sleep_seconds = 5 * attempt

                print(
                    f"Retrying login in "
                    f"{sleep_seconds}s..."
                )

                time.sleep(sleep_seconds)


    raise RuntimeError(
        f"API authentication failed after "
        f"{MAX_RETRIES} attempts: "
        f"{last_error}"
    )


# ============================================================
# FETCH CURRENT STOCK FOR ONE STORE
# ============================================================

def fetch_stock_report(
    session,
    token,
    store_id,
    store_name
):

    """
    Fetch CURRENT stock for one store.

    IMPORTANT:
        No date is sent to the stock API.

    The returned stock is labelled locally with
    RUN_DATE as snapshot_date.

    Returns:
        pandas.DataFrame

    Raises:
        RuntimeError for any invalid response.
    """

    params = {
        "store": int(store_id)
    }


    # ========================================================
    # IMPORTANT:
    #
    # Production stock.py uses:
    #
    #     Authorization: token
    #
    # NOT:
    #
    #     Authorization: Bearer token
    #
    # ========================================================

    headers = {
        "Authorization": token,
        "accept": "*/*"
    }


    last_error = None


    for attempt in range(1, MAX_RETRIES + 1):

        try:

            print(
                f"[{store_name}] "
                f"Attempt {attempt}/{MAX_RETRIES}"
            )


            # ------------------------------------------------
            # API CALL
            # ------------------------------------------------

            response = session.get(
                STOCK_URL,
                params=params,
                headers=headers,
                timeout=REQUEST_TIMEOUT
            )


            print(
                f"[{store_name}] "
                f"HTTP {response.status_code}"
            )


            # ------------------------------------------------
            # HTTP VALIDATION
            # ------------------------------------------------

            if response.status_code != 200:

                raise RuntimeError(
                    f"HTTP {response.status_code}: "
                    f"{response.text[:500]}"
                )


            encoded_payload = response.text.strip()


            if not encoded_payload:

                raise RuntimeError(
                    "API returned an empty response."
                )


            # ------------------------------------------------
            # BASE64 DECODE
            # ------------------------------------------------

            try:

                decoded_bytes = base64.b64decode(
                    encoded_payload,
                    validate=True
                )

            except Exception as exc:

                raise RuntimeError(
                    "API response is not valid Base64."
                ) from exc


            if len(decoded_bytes) < 100:

                raise RuntimeError(
                    f"Decoded payload is suspiciously "
                    f"small: {len(decoded_bytes)} bytes."
                )


            # ------------------------------------------------
            # DECODE CSV
            # ------------------------------------------------

            try:

                csv_text = decoded_bytes.decode(
                    "utf-8"
                )

            except UnicodeDecodeError as exc:

                raise RuntimeError(
                    "Stock CSV is not valid UTF-8."
                ) from exc


            if not csv_text.strip():

                raise RuntimeError(
                    "Decoded stock CSV is empty."
                )


            # ------------------------------------------------
            # PARSE CSV
            # ------------------------------------------------

            try:

                stock_pdf = pd.read_csv(
                    io.StringIO(csv_text),
                    dtype=str
                )

            except Exception as exc:

                raise RuntimeError(
                    f"Unable to parse stock CSV: {exc}"
                ) from exc


            # ------------------------------------------------
            # VALIDATE DATA
            # ------------------------------------------------

            if stock_pdf.empty:

                raise RuntimeError(
                    "Stock CSV contains zero rows."
                )


            # ------------------------------------------------
            # NORMALIZE COLUMN NAMES
            # ------------------------------------------------

            stock_pdf.columns = [
                str(c).strip()
                for c in stock_pdf.columns
            ]


            # ------------------------------------------------
            # METADATA
            # ------------------------------------------------

            stock_pdf["_store_id"] = str(
                store_id
            )

            stock_pdf["_store_name"] = str(
                store_name
            )

            # IMPORTANT:
            # This is the execution/snapshot date.
            #
            # The API itself does not receive this date.

            stock_pdf["_snapshot_date"] = (
                RUN_DATE.isoformat()
            )

            stock_pdf["_run_id"] = RUN_ID

            stock_pdf["_ingested_at"] = (
                datetime.now(timezone.utc)
                .isoformat()
            )


            print(
                f"[{store_name}] "
                f"SUCCESS - "
                f"{len(stock_pdf):,} rows"
            )


            return stock_pdf


        except Exception as exc:

            last_error = exc


            print(
                f"[{store_name}] "
                f"FAILED attempt {attempt}: "
                f"{exc}"
            )


            if attempt < MAX_RETRIES:

                sleep_seconds = 5 * attempt

                print(
                    f"[{store_name}] "
                    f"Retrying in "
                    f"{sleep_seconds}s..."
                )

                time.sleep(sleep_seconds)


    raise RuntimeError(
        f"Stock ingestion failed for store "
        f"{store_name} ({store_id}) after "
        f"{MAX_RETRIES} attempts: "
        f"{last_error}"
    )


# ============================================================
# READ PARTNER CONFIGURATION
# ============================================================

print(
    f"Reading partner configuration: "
    f"{PARTNER_FILE}"
)


partners_pdf = pd.read_csv(
    PARTNER_FILE,
    dtype=str
)


partners_pdf.columns = [
    str(c).strip()
    for c in partners_pdf.columns
]


print(
    f"Partner rows loaded: "
    f"{len(partners_pdf):,}"
)


print(
    "Partner columns:",
    list(partners_pdf.columns)
)


# ============================================================
# FIND STORE ID COLUMN
# ============================================================

store_id_candidates = [
    "store",
    "storeId",
    "store_id",
    "storeID"
]


store_name_candidates = [
    "storeName",
    "store_name",
    "store"
]


store_id_column = next(
    (
        c
        for c in store_id_candidates
        if c in partners_pdf.columns
    ),
    None
)


store_name_column = next(
    (
        c
        for c in store_name_candidates
        if c in partners_pdf.columns
    ),
    None
)


if store_id_column is None:

    raise RuntimeError(
        "Could not find a store ID column "
        "in partner.csv. "
        f"Available columns: "
        f"{list(partners_pdf.columns)}"
    )


if store_name_column is None:

    raise RuntimeError(
        "Could not find a store name column "
        "in partner.csv. "
        f"Available columns: "
        f"{list(partners_pdf.columns)}"
    )


print(
    f"Store ID column  : "
    f"{store_id_column}"
)


print(
    f"Store name column: "
    f"{store_name_column}"
)


# ============================================================
# CLEAN STORE LIST
# ============================================================

partners_pdf[store_id_column] = (
    partners_pdf[store_id_column]
    .astype(str)
    .str.strip()
)


partners_pdf[store_name_column] = (
    partners_pdf[store_name_column]
    .astype(str)
    .str.strip()
)


# ------------------------------------------------------------
# Remove blank store names
# ------------------------------------------------------------

partners_pdf = partners_pdf[
    partners_pdf[store_name_column].notna()
    &
    (
        partners_pdf[store_name_column]
        != ""
    )
].copy()


# ------------------------------------------------------------
# Deduplicate store configuration
# ------------------------------------------------------------

partners_pdf = (
    partners_pdf
    .drop_duplicates(
        subset=[
            store_id_column,
            store_name_column
        ]
    )
    .reset_index(drop=True)
)


print(
    f"Unique partner/store rows: "
    f"{len(partners_pdf):,}"
)


# ============================================================
# API SESSION
# ============================================================

session = requests.Session()


token = login()


print(
    "API authentication successful."
)


# ============================================================
# FETCH ALL STORES
# ============================================================

successful_frames = []

failed_stores = []

skipped_stores = []

start_time = time.time()


for _, row in partners_pdf.iterrows():

    store_id_raw = row[
        store_id_column
    ]

    store_name = row[
        store_name_column
    ]


    # ========================================================
    # MISSING STORE ID
    # ========================================================
    #
    # Same behavior as production:
    # missing ID is skipped.
    # ========================================================

    if (
        pd.isna(store_id_raw)
        or str(store_id_raw).strip() == ""
        or str(store_id_raw).lower()
        in {"nan", "none", "null"}
    ):

        print(
            f"[{store_name}] "
            f"SKIPPED - missing store ID"
        )


        skipped_stores.append({
            "store_name": store_name,
            "reason": "missing_store_id"
        })


        continue


    # ========================================================
    # VALIDATE STORE ID
    # ========================================================

    try:

        store_id = int(
            float(
                str(store_id_raw).strip()
            )
        )

    except Exception:

        print(
            f"[{store_name}] "
            f"FAILED - invalid store ID: "
            f"{store_id_raw}"
        )


        failed_stores.append({
            "store_id": str(
                store_id_raw
            ),
            "store_name": store_name,
            "reason": "invalid_store_id"
        })


        continue


    # ========================================================
    # FETCH CURRENT STOCK
    # ========================================================

    try:

        stock_pdf = fetch_stock_report(
            session=session,
            token=token,
            store_id=store_id,
            store_name=store_name
        )


        successful_frames.append(
            stock_pdf
        )


    except Exception as exc:

        print(
            f"[{store_name}] "
            f"FINAL FAILURE: {exc}"
        )


        failed_stores.append({
            "store_id": str(store_id),
            "store_name": store_name,
            "reason": str(exc)
        })


# ============================================================
# FETCH SUMMARY
# ============================================================

elapsed = (
    time.time() - start_time
)


print("=" * 70)
print("FETCH SUMMARY")
print("=" * 70)


print(
    f"Successful stores : "
    f"{len(successful_frames):,}"
)


print(
    f"Failed stores     : "
    f"{len(failed_stores):,}"
)


print(
    f"Skipped stores    : "
    f"{len(skipped_stores):,}"
)


print(
    f"Elapsed           : "
    f"{elapsed:.2f} seconds"
)


print("=" * 70)


# ============================================================
# FAIL-SAFE
# ============================================================
#
# CRITICAL:
#
# If ANY genuine store/API failure occurred,
# do NOT modify Delta.
#
# This prevents a partial snapshot from replacing
# the previous valid snapshot.
# ============================================================

if failed_stores:

    print(
        "FAILED STORES:"
    )


    for failure in failed_stores:

        print(
            f"  - "
            f"{failure['store_name']} "
            f"({failure['store_id']}): "
            f"{failure['reason']}"
        )


    raise RuntimeError(
        f"Stock ingestion aborted. "
        f"{len(failed_stores)} store(s) failed. "
        f"No Delta data was modified."
    )


# ============================================================
# ENSURE DATA EXISTS
# ============================================================

if not successful_frames:

    raise RuntimeError(
        "No stock data was successfully "
        "fetched. "
        "No Delta data was modified."
    )


# ============================================================
# COMBINE STORE DATA
# ============================================================

print(
    "Combining store datasets..."
)


combined_pdf = pd.concat(
    successful_frames,
    ignore_index=True,
    sort=False
)


if combined_pdf.empty:

    raise RuntimeError(
        "Combined stock dataset is empty. "
        "No Delta data was modified."
    )


# ============================================================
# CONVERT TO SPARK
# ============================================================

stock_df = spark.createDataFrame(
    combined_pdf
)


# ============================================================
# NORMALIZE METADATA TYPES
# ============================================================

stock_df = (
    stock_df

    .withColumn(
        "_store_id",
        F.col("_store_id")
        .cast("string")
    )

    .withColumn(
        "_store_name",
        F.col("_store_name")
        .cast("string")
    )

    .withColumn(
        "_snapshot_date",
        F.to_date(
            F.col("_snapshot_date")
        )
    )

    .withColumn(
        "_run_id",
        F.col("_run_id")
        .cast("string")
    )

    .withColumn(
        "_ingested_at",
        F.to_timestamp(
            "_ingested_at"
        )
    )
)


# ============================================================
# FINAL DATASET VALIDATION
# ============================================================

row_count = stock_df.count()


if row_count == 0:

    raise RuntimeError(
        "Final Spark DataFrame contains "
        "zero rows."
    )


store_count = (
    stock_df
    .select("_store_id")
    .distinct()
    .count()
)


print("=" * 70)
print("DATASET VALIDATION")
print("=" * 70)


print(
    f"Total rows        : "
    f"{row_count:,}"
)


print(
    f"Stores represented: "
    f"{store_count:,}"
)


print(
    f"Snapshot date     : "
    f"{RUN_DATE}"
)


print("=" * 70)


# ============================================================
# SNAPSHOT DATE VALIDATION
# ============================================================

invalid_snapshot_rows = (
    stock_df
    .filter(
        F.col("_snapshot_date")
        != F.lit(RUN_DATE)
    )
    .limit(1)
    .count()
)


if invalid_snapshot_rows > 0:

    raise RuntimeError(
        "Snapshot-date validation failed. "
        "Incoming data contains a snapshot "
        "date different from TARGET_DATE."
    )


# ============================================================
# STORE ID VALIDATION
# ============================================================

invalid_store_rows = (
    stock_df
    .filter(
        F.col("_store_id").isNull()
        |
        (
            F.trim(
                F.col("_store_id")
            ) == ""
        )
    )
    .limit(1)
    .count()
)


if invalid_store_rows > 0:

    raise RuntimeError(
        "Store ID validation failed. "
        "Incoming data contains missing "
        "store IDs."
    )


# ============================================================
# CREATE BRONZE TABLE IF REQUIRED
# ============================================================

if not spark.catalog.tableExists(
    BRONZE_TABLE
):

    print(
        f"🆕 Creating Bronze table: "
        f"{BRONZE_TABLE}"
    )


    (
        stock_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(
            BRONZE_TABLE
        )
    )


else:

    print(
        f"Bronze table already exists: "
        f"{BRONZE_TABLE}"
    )


# ============================================================
# SCHEMA SAFETY
# ============================================================

existing_columns = [
    field.name
    for field
    in spark.table(
        BRONZE_TABLE
    ).schema.fields
]


incoming_columns = (
    stock_df.columns
)


print(
    "Existing Bronze columns:"
)

print(
    existing_columns
)


print(
    "Incoming columns:"
)

print(
    incoming_columns
)


# ============================================================
# SCHEMA EVOLUTION
# ============================================================

new_columns = [
    c
    for c in incoming_columns
    if c not in existing_columns
]


if new_columns:

    print(
        "New columns detected:",
        new_columns
    )


    for column_name in new_columns:

        field_type = next(
            field.dataType
            for field
            in stock_df.schema.fields
            if field.name == column_name
        )


        spark.sql(
            f"""
            ALTER TABLE {BRONZE_TABLE}
            ADD COLUMNS (
                `{column_name}`
                {field_type.simpleString()}
            )
            """
        )


# ============================================================
# ALIGN DATAFRAME TO BRONZE SCHEMA
# ============================================================

bronze_schema = (
    spark.table(
        BRONZE_TABLE
    ).schema
)


bronze_columns = [
    field.name
    for field in bronze_schema.fields
]


for field in bronze_schema.fields:

    if field.name not in stock_df.columns:

        stock_df = stock_df.withColumn(
            field.name,
            F.lit(None).cast(
                field.dataType
            )
        )


# Keep exactly the Bronze column order

stock_df = stock_df.select(
    *bronze_columns
)


# ============================================================
# FINAL PRE-WRITE VALIDATION
# ============================================================

incoming_rows = stock_df.count()


if incoming_rows != row_count:

    raise RuntimeError(
        "Row count changed during schema "
        "alignment."
    )


incoming_stores = (
    stock_df
    .select("_store_id")
    .distinct()
    .count()
)


if incoming_stores != store_count:

    raise RuntimeError(
        "Store count changed during schema "
        "alignment."
    )


# ============================================================
# IDEMPOTENT DELTA REPLACEMENT
# ============================================================
#
# IMPORTANT:
#
# We only reach this point if:
#
#   - every store succeeded
#   - dataset is non-empty
#   - snapshot date is valid
#   - store IDs are valid
#   - schema alignment succeeded
#
# Therefore a failed API run never deletes the previous
# snapshot.
#
# A successful rerun replaces only TARGET_DATE.
# ============================================================

print()
print(
    f"♻️ Replacing existing stock snapshot "
    f"for {RUN_DATE}..."
)


spark.sql(
    f"""
    DELETE FROM {BRONZE_TABLE}
    WHERE _snapshot_date =
          DATE('{RUN_DATE.isoformat()}')
    """
)


# ============================================================
# WRITE
# ============================================================

(
    stock_df
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(
        BRONZE_TABLE
    )
)


# ============================================================
# POST-WRITE VALIDATION
# ============================================================

written_df = (
    spark.table(
        BRONZE_TABLE
    )
    .filter(
        F.col("_snapshot_date")
        == F.lit(RUN_DATE)
    )
)


written_rows = (
    written_df.count()
)


written_stores = (
    written_df
    .select("_store_id")
    .distinct()
    .count()
)


# ============================================================
# VALIDATE ROW COUNT
# ============================================================

if written_rows == 0:

    raise RuntimeError(
        "Post-write validation failed: "
        "zero rows found for snapshot date."
    )


# ============================================================
# VALIDATE STORE COUNT
# ============================================================

if written_stores == 0:

    raise RuntimeError(
        "Post-write validation failed: "
        "zero stores found for snapshot date."
    )


if written_rows != row_count:

    raise RuntimeError(
        f"Post-write row count mismatch. "
        f"Expected {row_count:,}, "
        f"found {written_rows:,}."
    )


if written_stores != store_count:

    raise RuntimeError(
        f"Post-write store count mismatch. "
        f"Expected {store_count:,}, "
        f"found {written_stores:,}."
    )


# ============================================================
# FINAL REPORT
# ============================================================

print()

print("=" * 70)
print("✅ STOCK INGESTION COMPLETED SUCCESSFULLY")
print("=" * 70)

print(
    f"Run ID          : {RUN_ID}"
)

print(
    f"Snapshot date   : {RUN_DATE}"
)

print(
    f"Stores fetched  : {written_stores:,}"
)

print(
    f"Rows written    : {written_rows:,}"
)

print(
    f"Skipped stores  : "
    f"{len(skipped_stores):,}"
)

print(
    f"Elapsed seconds : "
    f"{elapsed:.2f}"
)

print("=" * 70)

# COMMAND ----------

