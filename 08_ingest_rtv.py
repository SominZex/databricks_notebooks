# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# =============================================================================
# 08_ingest_rtv
#
# RTV INGESTION — WEEKLY MONDAY-SCHEDULED INGESTION
#
# PURPOSE
# -------
# Runs every Monday and fetches the previous complete Monday-Sunday week.
#
# Example:
#   Execution date : 2026-09-21 (Monday)
#   Report week    : 2026-09-14 through 2026-09-20
#   API request     : fromDate=2026-09-14, toDate=2026-09-21
#
# RTV API semantics:
#   fromDate <= RTV_Date < toDate
#
# DATA-SAFETY DESIGN
# ------------------
# 1. Fetch the complete previous week for every configured store.
# 2. Fail before touching Bronze if any genuine store/API request fails.
# 3. Validate all returned dates and keep only the requested week.
# 4. Never delete the whole table.
# 5. Replace only the weekly partition created by this notebook.
# 6. Preserve legacy/daily rows by using _ingestion_mode='weekly' and
#    _week_start_date as the weekly replacement key.
# 7. If the API returns no rows for the whole week, preserve existing weekly
#    data and stop safely.
#
# IMPORTANT:
# The existing 00_config TARGET_DATE (yesterday) is intentionally not used
# for weekly boundaries. The weekly window is derived from the actual run date.
# =============================================================================


import requests
import pandas as pd
import io
import base64
import binascii
import re
import uuid

from datetime import datetime, timedelta

from pyspark.sql import functions as F


# =============================================================================
# 1. CONFIGURATION
# =============================================================================

SOURCE_TABLE = f"{catalog_name}.{schema_name}.rtv_bronze"

PARTNER_FILE = (
    f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/partner.csv"
)

RUN_ID = str(uuid.uuid4())


# =============================================================================
# 2. DYNAMIC PREVIOUS-WEEK WINDOW
# =============================================================================

EXECUTION_DATE = datetime.now().date()

# Monday = 0, Sunday = 6
CURRENT_MONDAY = (
    EXECUTION_DATE
    - timedelta(days=EXECUTION_DATE.weekday())
)

# Previous complete Monday-Sunday week
REPORT_START_DATE = CURRENT_MONDAY - timedelta(days=7)
REPORT_END_DATE = CURRENT_MONDAY - timedelta(days=1)

# API toDate is exclusive, so use the current Monday.
FROM_DATE = REPORT_START_DATE
TO_DATE = CURRENT_MONDAY

# Stable weekly replacement key.
WEEK_START_DATE = REPORT_START_DATE
WEEK_END_DATE = REPORT_END_DATE

print("=" * 100)
print("WEEKLY RTV INGESTION")
print("=" * 100)
print(f"Target table       : {SOURCE_TABLE}")
print(f"Run ID             : {RUN_ID}")
print(f"Execution date     : {EXECUTION_DATE}")
print(f"Report week start  : {REPORT_START_DATE}")
print(f"Report week end    : {REPORT_END_DATE}")
print(f"API from date      : {FROM_DATE}")
print(f"API to date        : {TO_DATE} (exclusive)")
print("API window         : Previous complete Monday-Sunday week")
print("=" * 100)


# =============================================================================
# 3. WEEKLY METADATA COLUMNS
# =============================================================================

WEEKLY_METADATA_COLUMNS = {
    "_ingestion_mode": "weekly",
    "_week_start_date": WEEK_START_DATE,
    "_week_end_date": WEEK_END_DATE,
}

# =============================================================================
# 4. AUTHENTICATION
# =============================================================================

def authenticate():

    print("Authenticating...")

    response = requests.post(
        f"{API_BASE_URL}/login",
        json={
            "username": API_USERNAME,
            "password": API_PASSWORD
        },
        timeout=30
    )

    if response.status_code not in (200, 201):

        raise RuntimeError(
            f"Authentication failed: "
            f"HTTP {response.status_code} - "
            f"{response.text[:500]}"
        )

    try:

        payload = response.json()

    except Exception as exc:

        raise RuntimeError(
            f"Authentication response is not valid JSON: {exc}"
        )

    token = payload.get("token")

    if not token:

        raise RuntimeError(
            "Authentication succeeded but no token was returned."
        )

    print("✅ Authentication successful")

    return token


# =============================================================================
# 5. LOAD PARTNER CONFIGURATION
# =============================================================================

def load_partners(path):

    print("\nLoading partner configuration:")
    print(path)

    try:

        partners = pd.read_csv(path)

    except Exception as exc:

        raise RuntimeError(
            f"Unable to read partner.csv: {exc}"
        )

    required_columns = [
        "storeName",
        "email",
        "wa_number",
        "store"
    ]

    missing_columns = [
        c
        for c in required_columns
        if c not in partners.columns
    ]

    if missing_columns:

        raise RuntimeError(
            f"partner.csv is missing required columns: "
            f"{missing_columns}"
        )

    partners["storeName"] = (
        partners["storeName"]
        .astype("string")
        .str.strip()
    )

    partners["store"] = (
        partners["store"]
        .astype("string")
        .str.strip()
    )

    partners = partners[
        partners["storeName"].notna()
    ].copy()

    partners = partners[
        partners["storeName"] != ""
    ].copy()

    print(
        f"Configured stores : {len(partners):,}"
    )

    return partners


partners = load_partners(PARTNER_FILE)


# =============================================================================
# 6. NORMALIZE COLUMN NAMES
# =============================================================================

def normalize_columns(df):

    df.columns = [
        str(c).strip()
        for c in df.columns
    ]

    return df


# =============================================================================
# 7. DECODE API RESPONSE
# =============================================================================

def decode_csv_response(response):

    if response.status_code != 200:

        raise RuntimeError(
            f"HTTP {response.status_code}: "
            f"{response.text[:500]}"
        )

    content = response.content

    if not content:

        return pd.DataFrame()

    # -------------------------------------------------------------------------
    # Decode UTF-8
    # -------------------------------------------------------------------------

    try:

        text = content.decode("utf-8")

    except UnicodeDecodeError:

        text = None


    if text is not None:

        stripped = text.strip()

        if not stripped:

            return pd.DataFrame()

        # ---------------------------------------------------------------------
        # Try Base64
        # ---------------------------------------------------------------------

        clean = re.sub(
            r"\s+",
            "",
            stripped
        )

        try:

            decoded = base64.b64decode(
                clean,
                validate=True
            )

            decoded_text = decoded.decode("utf-8")

            try:

                df = pd.read_csv(
                    io.StringIO(decoded_text),
                    low_memory=False
                )

                return normalize_columns(df)

            except Exception:

                pass

        except (
            binascii.Error,
            ValueError,
            UnicodeDecodeError
        ):

            pass


        # ---------------------------------------------------------------------
        # Plain CSV
        # ---------------------------------------------------------------------

        try:

            df = pd.read_csv(
                io.StringIO(text),
                low_memory=False
            )

            return normalize_columns(df)

        except pd.errors.EmptyDataError:

            return pd.DataFrame()

        except Exception as exc:

            raise RuntimeError(
                f"Unable to parse API response as CSV: {exc}"
            )


    # -------------------------------------------------------------------------
    # Binary/Base64 fallback
    # -------------------------------------------------------------------------

    try:

        decoded = base64.b64decode(
            content,
            validate=True
        )

        decoded_text = decoded.decode("utf-8")

        df = pd.read_csv(
            io.StringIO(decoded_text),
            low_memory=False
        )

        return normalize_columns(df)

    except Exception as exc:

        raise RuntimeError(
            f"Unable to decode API response: {exc}"
        )


# =============================================================================
# 8. FETCH RTV FOR ONE STORE
# =============================================================================

def fetch_rtv(
    token,
    store_id,
    store_name
):

    # -------------------------------------------------------------------------
    # The RTV API uses:
    #
    #     fromDate <= transaction_date < toDate
    #
    # TO_DATE is the next Monday and is exclusive.
    # -------------------------------------------------------------------------

    params = {
        "storeId": str(store_id),
        "fromDate": FROM_DATE.strftime("%Y-%m-%d"),
        "toDate": TO_DATE.strftime("%Y-%m-%d")
    }

    response = requests.get(
        f"{API_BASE_URL}/rtvstore-stocks/RTVreport",

        params=params,

        headers={
            "accept": "*/*",
            "Authorization": token
        },

        timeout=60
    )

    df = decode_csv_response(response)

    if df.empty:

        return df

    df["_configured_store_name"] = store_name
    df["_configured_store_id"] = str(store_id)

    return df


# =============================================================================
# 9. VALIDATE RTV DATAFRAME
# =============================================================================

def validate_rtv_dataframe(
    df,
    store_name
):

    if df is None:

        raise RuntimeError(
            f"{store_name}: API returned None"
        )

    # HTTP 200 + zero rows is valid.
    if df.empty:

        return True

    required_columns = [
        "rtvId",
        "Date",
        "productId",
        "productName",
        "barcode",
        "quantity",
        "price",
        "totalAmount",
        "storeName"
    ]

    missing_columns = [
        c
        for c in required_columns
        if c not in df.columns
    ]

    if missing_columns:

        raise RuntimeError(
            f"{store_name}: "
            f"RTV response missing columns: "
            f"{missing_columns}"
        )

    return True


# =============================================================================
# 10. AUTHENTICATE
# =============================================================================

token = authenticate()


# =============================================================================
# 11. FETCH ALL STORES
# =============================================================================

all_frames = []

successful_stores = []
stores_with_rtv = []
stores_without_rtv = []

failed_stores = []
skipped_stores = []

total_api_rows = 0


print("\n" + "=" * 90)
print("STORE-LEVEL RTV INGESTION")
print("=" * 90)


for index, row in partners.iterrows():

    store_name = str(
        row["storeName"]
    ).strip()

    store_id_raw = row["store"]


    # =========================================================================
    # Missing store ID
    # =========================================================================

    if (
        pd.isna(store_id_raw)
        or str(store_id_raw).strip() == ""
        or str(store_id_raw).strip().lower() == "nan"
    ):

        skipped_stores.append({
            "storeName": store_name,
            "reason": "Missing store ID"
        })

        print(
            f"[{index + 1}/{len(partners)}] "
            f"⚠️ SKIPPED | {store_name} | "
            f"Missing store ID"
        )

        continue


    store_id = str(
        store_id_raw
    ).strip()


    # =========================================================================
    # FETCH
    # =========================================================================

    try:

        df = fetch_rtv(
            token=token,
            store_id=store_id,
            store_name=store_name
        )

        validate_rtv_dataframe(
            df,
            store_name
        )

        successful_stores.append(
            store_name
        )


        if df.empty:

            stores_without_rtv.append(
                store_name
            )

            print(
                f"[{index + 1}/{len(partners)}] "
                f"✅ {store_name} | "
                f"HTTP 200 | "
                f"0 RTV rows"
            )

            continue


        stores_with_rtv.append(
            store_name
        )

        row_count = len(df)

        total_api_rows += row_count

        all_frames.append(df)

        print(
            f"[{index + 1}/{len(partners)}] "
            f"✅ {store_name} | "
            f"{row_count:,} RTV rows"
        )


    except Exception as exc:

        failed_stores.append({
            "storeName": store_name,
            "store_id": store_id,
            "error": str(exc)
        })

        print(
            f"[{index + 1}/{len(partners)}] "
            f"❌ FAILED | {store_name} | "
            f"{exc}"
        )


# =============================================================================
# 12. API SUMMARY
# =============================================================================

print("\n" + "=" * 90)
print("API INGESTION SUMMARY")
print("=" * 90)

print(
    f"Report week              : {REPORT_START_DATE} → {REPORT_END_DATE}"
)

print(
    f"API window               : "
    f"{FROM_DATE} → {TO_DATE}"
)

print(
    f"Stores configured        : "
    f"{len(partners):,}"
)

print(
    f"Stores successful        : "
    f"{len(successful_stores):,}"
)

print(
    f"Stores with RTV          : "
    f"{len(stores_with_rtv):,}"
)

print(
    f"Stores with no RTV       : "
    f"{len(stores_without_rtv):,}"
)

print(
    f"Stores skipped           : "
    f"{len(skipped_stores):,}"
)

print(
    f"Stores failed            : "
    f"{len(failed_stores):,}"
)

print(
    f"API rows received        : "
    f"{total_api_rows:,}"
)


# =============================================================================
# 13. FAIL BEFORE MODIFYING BRONZE
# =============================================================================

if failed_stores:

    print(
        "\n❌ RTV ingestion failed."
    )

    for item in failed_stores:

        print(
            f"   - {item['storeName']} "
            f"(store_id={item['store_id']}): "
            f"{item['error']}"
        )

    print(
        "\n⚠️ Existing Bronze data has NOT been modified."
    )

    raise RuntimeError(
        f"RTV ingestion failed for "
        f"{len(failed_stores)} store(s). "
        f"Bronze was not modified."
    )


# =============================================================================
# 14. COMBINE API DATA
# =============================================================================

if all_frames:

    combined_pd = pd.concat(
        all_frames,
        ignore_index=True
    )

else:

    combined_pd = pd.DataFrame()


print("\n" + "=" * 90)
print("COMBINED API DATA")
print("=" * 90)

print(
    f"Rows returned by API : "
    f"{len(combined_pd):,}"
)


# =============================================================================
# 15. PARSE ACTUAL RTV DATE
# =============================================================================

if not combined_pd.empty:

    combined_pd = normalize_columns(
        combined_pd
    )

    if "Date" not in combined_pd.columns:

        raise RuntimeError(
            "RTV API returned data but "
            "the Date column is missing."
        )

    combined_pd["_rtv_date"] = pd.to_datetime(
        combined_pd["Date"],
        errors="coerce"
    ).dt.date

    invalid_dates = combined_pd[
        combined_pd["_rtv_date"].isna()
    ]

    if len(invalid_dates) > 0:

        raise RuntimeError(
            f"{len(invalid_dates):,} RTV rows "
            f"have invalid Date values."
        )


# =============================================================================
# 16. SHOW API DATE DISTRIBUTION
# =============================================================================

if not combined_pd.empty:

    print("\nAPI rows by actual RTV date:")

    date_summary = (
        combined_pd
        .groupby("_rtv_date")
        .size()
        .sort_index()
    )

    for rtv_date, count in date_summary.items():

        print(
            f"   {rtv_date} : "
            f"{count:,} rows"
        )


# =============================================================================
# 17. KEEP ONLY THE PREVIOUS COMPLETE WEEK
# =============================================================================

if not combined_pd.empty:
    target_pd = combined_pd[
        (combined_pd["_rtv_date"] >= REPORT_START_DATE)
        & (combined_pd["_rtv_date"] <= REPORT_END_DATE)
    ].copy()
else:
    target_pd = pd.DataFrame()

print("\n" + "=" * 100)
print("WEEKLY DATE FILTER")
print("=" * 100)
print(f"Report week start    : {REPORT_START_DATE}")
print(f"Report week end      : {REPORT_END_DATE}")
print(f"Rows in report week  : {len(target_pd):,}")


# =============================================================================
# 18. CHECK EXISTING WEEKLY BRONZE PARTITION
# =============================================================================

table_exists = spark.catalog.tableExists(SOURCE_TABLE)

existing_week_count = 0

if table_exists:
    existing_columns = {
        field.name
        for field in spark.table(SOURCE_TABLE).schema.fields
    }

    # Add weekly metadata columns without changing or deleting existing rows.
    missing_metadata_columns = {
        "_ingestion_mode": "STRING",
        "_week_start_date": "DATE",
        "_week_end_date": "DATE",
    }

    for column_name, data_type in missing_metadata_columns.items():
        if column_name not in existing_columns:
            spark.sql(
                f"ALTER TABLE {SOURCE_TABLE} "
                f"ADD COLUMNS (`{column_name}` {data_type})"
            )

    existing_week_count = (
        spark.table(SOURCE_TABLE)
        .filter(
            (F.col("_ingestion_mode") == F.lit("weekly"))
            & (
                F.to_date(F.col("_week_start_date"))
                == F.lit(WEEK_START_DATE)
            )
        )
        .count()
    )

print(f"Existing weekly rows : {existing_week_count:,}")


# =============================================================================
# 19. WEEKLY DATA SAFETY CHECK
# =============================================================================

# A completely empty weekly response must never delete an existing weekly
# partition. This protects against an API outage, incorrect date behavior,
# or an unexpected empty response.

if target_pd.empty:
    if existing_week_count > 0:
        raise RuntimeError(
            f"API returned no RTV rows for report week "
            f"{REPORT_START_DATE} to {REPORT_END_DATE}, but Bronze already "
            f"contains {existing_week_count:,} rows for that week. "
            "Existing weekly data was preserved."
        )

    print("\n⚠️ No RTV rows returned for the requested week.")
    print("Bronze modified   : NO")
    print(f"Report week       : {REPORT_START_DATE} → {REPORT_END_DATE}")
    dbutils.notebook.exit(
        "SUCCESS: No RTV records for the requested reporting week."
    )


# =============================================================================
# 20. PREPARE WEEKLY DATA
# =============================================================================

target_pd["_ingestion_mode"] = "weekly"
target_pd["_week_start_date"] = WEEK_START_DATE
target_pd["_week_end_date"] = WEEK_END_DATE

# Keep execution metadata for traceability, but do not use it as the weekly
# replacement key because legacy daily data may already use this column.
target_pd["_execution_date"] = EXECUTION_DATE
target_pd["_from_date"] = FROM_DATE
target_pd["_to_date"] = TO_DATE
target_pd["_run_id"] = RUN_ID

target_pd = target_pd.drop(
    columns=["_rtv_date"],
    errors="ignore"
)


# =============================================================================
# 21. CONVERT NUMERIC COLUMNS
# =============================================================================

numeric_columns = [
    "productId",
    "barcode",
    "quantity",
    "price",
    "totalAmount"
]


for column_name in numeric_columns:

    if column_name in target_pd.columns:

        target_pd[column_name] = pd.to_numeric(
            target_pd[column_name],
            errors="coerce"
        )


# =============================================================================
# 22. PARSE DATE
# =============================================================================

if "Date" in target_pd.columns:

    target_pd["Date"] = pd.to_datetime(
        target_pd["Date"],
        errors="coerce"
    ).dt.date


# =============================================================================
# 23. CREATE SPARK DATAFRAME
# =============================================================================

target_spark_df = spark.createDataFrame(
    target_pd
)


# =============================================================================
# 24. PRE-WRITE VALIDATION
# =============================================================================

target_row_count = target_spark_df.count()


invalid_target_rows = (
    target_spark_df
    .filter(
        F.to_date(F.col("Date")).isNull()
        | (F.to_date(F.col("Date")) < F.lit(REPORT_START_DATE))
        | (F.to_date(F.col("Date")) > F.lit(REPORT_END_DATE))
    )
    .count()
)


print("\n" + "=" * 90)
print("PRE-WRITE VALIDATION")
print("=" * 90)

print(
    f"Target-date rows : "
    f"{target_row_count:,}"
)

print(
    f"Invalid dates    : "
    f"{invalid_target_rows:,}"
)


if target_row_count == 0:

    raise RuntimeError(
        "Target dataframe contains zero rows."
    )


if invalid_target_rows > 0:

    raise RuntimeError(
        f"Refusing to write: "
        f"{invalid_target_rows:,} rows have dates outside "
        f"{REPORT_START_DATE} to {REPORT_END_DATE}."
    )


# =============================================================================
# 25. CREATE BRONZE TABLE IF NECESSARY
# =============================================================================

if not table_exists:

    print(
        f"\nℹ️ Bronze table does not exist."
    )

    print(
        f"Creating {SOURCE_TABLE}"
    )

    (
        target_spark_df
        .write
        .format("delta")
        .mode("append")
        .partitionBy("_week_start_date")
        .saveAsTable(SOURCE_TABLE)
    )

    print(
        "✅ Bronze table created."
    )


# =============================================================================
# 26. EXISTING TABLE — IDEMPOTENT REPLACEMENT
# =============================================================================

else:

    print(
        f"\nExisting Bronze table found."
    )

    print(
        f"Replacing only weekly partition: "
        f"{WEEK_START_DATE} → {WEEK_END_DATE}"
    )


    # -------------------------------------------------------------------------
    # DELETE ONLY TARGET DATE
    # -------------------------------------------------------------------------

    spark.sql(
        f"""
        DELETE FROM {SOURCE_TABLE}
        WHERE _ingestion_mode = 'weekly'
          AND TO_DATE(_week_start_date) = DATE '{WEEK_START_DATE}'
        """
    )


    print(
        f"🗑️ Cleared existing rows for "
        f"{REPORT_START_DATE} → {REPORT_END_DATE}"
    )


    # -------------------------------------------------------------------------
    # MATCH TARGET SCHEMA
    # -------------------------------------------------------------------------

    target_schema = spark.table(
        SOURCE_TABLE
    ).schema


    target_columns = [
        field.name
        for field in target_schema.fields
    ]


    staging_df = target_spark_df


    # -------------------------------------------------------------------------
    # Add missing target columns
    # -------------------------------------------------------------------------

    for field in target_schema.fields:

        column_name = field.name

        if column_name not in staging_df.columns:

            staging_df = staging_df.withColumn(
                column_name,
                F.lit(None).cast(
                    field.dataType
                )
            )


    # -------------------------------------------------------------------------
    # Exact target column order
    # -------------------------------------------------------------------------

    staging_df = staging_df.select(
        target_columns
    )


    staging_df.createOrReplaceTempView(
        "rtv_bronze_staging"
    )


    column_list = ", ".join(
        f"`{c}`"
        for c in target_columns
    )


    spark.sql(
        f"""
        INSERT INTO {SOURCE_TABLE}
        ({column_list})
        SELECT {column_list}
        FROM rtv_bronze_staging
        """
    )


    spark.catalog.dropTempView(
        "rtv_bronze_staging"
    )


    print(
        f"✅ Inserted "
        f"{target_row_count:,} weekly RTV rows."
    )


# =============================================================================
# 27. FINAL VALIDATION
# =============================================================================

final_df = (
    spark.table(SOURCE_TABLE)
    .filter(
        (F.col("_ingestion_mode") == F.lit("weekly"))
        & (
            F.to_date(F.col("_week_start_date"))
            == F.lit(WEEK_START_DATE)
        )
    )
)

final_count = final_df.count()

print("\n" + "=" * 100)
print("FINAL WEEKLY BRONZE VALIDATION")
print("=" * 100)
print(f"Report week       : {REPORT_START_DATE} → {REPORT_END_DATE}")
print(f"Rows              : {final_count:,}")
print(f"Run ID            : {RUN_ID}")

if final_count == 0:
    raise RuntimeError(
        f"Weekly Bronze write completed but 0 rows exist for "
        f"week {REPORT_START_DATE} to {REPORT_END_DATE}."
    )


# =============================================================================
# 28. FINAL DATE AND PARTITION VALIDATION
# =============================================================================

invalid_final_dates = (
    final_df
    .filter(
        F.to_date(F.col("Date")).isNull()
        | (F.to_date(F.col("Date")) < F.lit(REPORT_START_DATE))
        | (F.to_date(F.col("Date")) > F.lit(REPORT_END_DATE))
    )
    .count()
)

invalid_final_partitions = (
    final_df
    .filter(
        (F.to_date(F.col("_week_start_date")) != F.lit(WEEK_START_DATE))
        | (F.to_date(F.col("_week_end_date")) != F.lit(WEEK_END_DATE))
    )
    .count()
)

if invalid_final_dates > 0:
    raise RuntimeError(
        f"Final weekly validation failed: {invalid_final_dates:,} rows "
        f"have Date outside {REPORT_START_DATE} to {REPORT_END_DATE}."
    )

if invalid_final_partitions > 0:
    raise RuntimeError(
        f"Final weekly partition validation failed: "
        f"{invalid_final_partitions:,} rows have incorrect week metadata."
    )


# =============================================================================
# 29. FINAL METRICS
# =============================================================================

metrics = (
    final_df
    .agg(
        F.count("*").alias("rows"),

        F.countDistinct(
            "rtvId"
        ).alias("rtv_ids"),

        F.countDistinct(
            "storeName"
        ).alias("stores"),

        F.countDistinct(
            "productId"
        ).alias("products"),

        F.sum(
            F.col("quantity").cast("double")
        ).alias("quantity"),

        F.sum(
            F.col("totalAmount").cast("double")
        ).alias("total_amount")
    )
    .collect()[0]
)


# =============================================================================
# 30. FINAL SUCCESS
# =============================================================================

print("\n" + "=" * 90)
print("✅ RTV INGESTION COMPLETED SUCCESSFULLY")
print("=" * 90)

print(
    f"Report week       : {REPORT_START_DATE} → {REPORT_END_DATE}"
)

print(
    f"API window        : "
    f"{FROM_DATE} → {TO_DATE}"
)

print(
    f"Stores configured : "
    f"{len(partners):,}"
)

print(
    f"Stores successful : "
    f"{len(successful_stores):,}"
)

print(
    f"Stores with RTV   : "
    f"{len(stores_with_rtv):,}"
)

print(
    f"Stores no RTV     : "
    f"{len(stores_without_rtv):,}"
)

print(
    f"Stores skipped    : "
    f"{len(skipped_stores):,}"
)

print(
    f"Stores failed     : "
    f"{len(failed_stores):,}"
)

print(
    f"API rows received : "
    f"{total_api_rows:,}"
)

print(
    f"Target rows       : "
    f"{final_count:,}"
)

print(
    f"RTV IDs           : "
    f"{metrics['rtv_ids']:,}"
)

print(
    f"Stores in target  : "
    f"{metrics['stores']:,}"
)

print(
    f"Products          : "
    f"{metrics['products']:,}"
)

print(
    f"Quantity          : "
    f"{metrics['quantity']}"
)

print(
    f"Total amount      : "
    f"{metrics['total_amount']}"
)

print(
    f"Run ID            : "
    f"{RUN_ID}"
)

print("=" * 90)

# COMMAND ----------

