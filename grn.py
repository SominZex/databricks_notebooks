# Databricks notebook source
# Databricks notebook source
# =============================================================================
# GRN ETL — Production Grade / Idempotent Daily Snapshot Loader
# =============================================================================
#
# Properties:
#   - Idempotent
#   - Atomic date-partition replacement
#   - Safe reruns
#   - API retry + authentication refresh
#   - Date validation
#   - Schema validation
#   - Staging isolation
#   - Post-write validation
#   - Multi-date backfill support
#   - Does NOT use (Date, grnId) as a MERGE key
#
# IMPORTANT:
#
# A single grnId can contain multiple product lines.
#
# Therefore:
#
#       (Date, grnId)
#
# is NOT a valid row-level identity for GRN.
#
# GRN is treated as a daily API snapshot.
#
# For every processed date:
#
#       API snapshot
#             ↓
#       clean + validate
#             ↓
#       staging
#             ↓
#       atomic Date partition replacement
#             ↓
#       validation
#
# Re-running the same date replaces that date's snapshot instead of
# inserting duplicate rows.
#
# =============================================================================


# COMMAND ----------
# MAGIC %md
# MAGIC ## CELL 1 — DDL
# MAGIC
# MAGIC Creates:
# MAGIC
# MAGIC - billing.sales_data.grn_data
# MAGIC - billing.sales_data.grn_data_staging
# MAGIC
# MAGIC The target is partitioned by Date because GRN is processed as a
# MAGIC daily snapshot.
# MAGIC
# MAGIC This cell is safe to execute repeatedly.


# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


# =============================================================================
# Configuration
# =============================================================================

CATALOG = "billing"
SCHEMA = "sales_data"

FULL_TABLE = f"{CATALOG}.{SCHEMA}.grn_data"
STAGING_TABLE = f"{CATALOG}.{SCHEMA}.grn_data_staging"


# =============================================================================
# Create schema
# =============================================================================

spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}
    """
)

print(
    f"Schema '{CATALOG}.{SCHEMA}' ready."
)


# =============================================================================
# Target table
# =============================================================================
#
# IMPORTANT:
#   Date is the physical partition.
#
# We do NOT define a MERGE key here because the raw GRN source does not
# guarantee that grnId is unique per Date.
#
# =============================================================================

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {FULL_TABLE} (

        Date             DATE,
        Time             STRING,

        acess            DECIMAL(18, 6),
        acessAmount      DECIMAL(18, 6),
        amountNet        DECIMAL(18, 6),

        barcode          STRING,
        brandName        STRING,
        categoryName     STRING,

        cess             DECIMAL(18, 6),
        cessAmount       DECIMAL(18, 6),

        costPrice        DECIMAL(18, 6),

        grnId            STRING,

        gst              DECIMAL(18, 6),
        gstAmount        DECIMAL(18, 6),

        invoiceNumber    STRING,

        productName      STRING,

        recievedQuantity BIGINT,

        sellingPrice     DECIMAL(18, 6),

        storeName        STRING,

        totalCost        DECIMAL(18, 6),
        totalTaxes       DECIMAL(18, 6),

        user             STRING,
        vendor           STRING,
        vendorGst        STRING

    )
    USING DELTA
    PARTITIONED BY (Date)

    TBLPROPERTIES (
        'delta.enableChangeDataFeed'       = 'false',
        'delta.autoOptimize.optimizeWrite' = 'true'
    )

    COMMENT 'GRN daily snapshot data ingested from The New Shop API'
    """
)

print(
    f"Table '{FULL_TABLE}' ready."
)


# =============================================================================
# Staging table
# =============================================================================
#
# Staging is disposable.
#
# Every processing date overwrites this table before the target partition
# is replaced.
#
# =============================================================================

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {STAGING_TABLE} (

        Date             DATE,
        Time             STRING,

        acess            DECIMAL(18, 6),
        acessAmount      DECIMAL(18, 6),
        amountNet        DECIMAL(18, 6),

        barcode          STRING,
        brandName        STRING,
        categoryName     STRING,

        cess             DECIMAL(18, 6),
        cessAmount       DECIMAL(18, 6),

        costPrice        DECIMAL(18, 6),

        grnId            STRING,

        gst              DECIMAL(18, 6),
        gstAmount        DECIMAL(18, 6),

        invoiceNumber    STRING,

        productName      STRING,

        recievedQuantity BIGINT,

        sellingPrice     DECIMAL(18, 6),

        storeName        STRING,

        totalCost        DECIMAL(18, 6),
        totalTaxes       DECIMAL(18, 6),

        user             STRING,
        vendor           STRING,
        vendorGst        STRING

    )
    USING DELTA

    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true'
    )

    COMMENT 'Disposable staging table for GRN daily snapshot replacement'
    """
)

print(
    f"Staging table '{STAGING_TABLE}' ready."
)

print(
    "\nDDL complete."
)


# COMMAND ----------
# MAGIC %md
# MAGIC ## CELL 2 — Daily GRN ETL
# MAGIC
# MAGIC Processing model:
# MAGIC
# MAGIC 1. Authenticate
# MAGIC 2. Download one business date
# MAGIC 3. Decode CSV
# MAGIC 4. Clean and normalize
# MAGIC 5. Validate date
# MAGIC 6. Write staging
# MAGIC 7. Atomically replace only that Date partition
# MAGIC 8. Validate target
# MAGIC
# MAGIC Rerunning the same date is safe.


# COMMAND ----------

import requests
import base64
import binascii
import re
import io
import time
import traceback

from datetime import datetime, timedelta

import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.getOrCreate()


# =============================================================================
# Configuration
# =============================================================================

CATALOG = "billing"
SCHEMA = "sales_data"

FULL_TABLE = f"{CATALOG}.{SCHEMA}.grn_data"
STAGING_TABLE = f"{CATALOG}.{SCHEMA}.grn_data_staging"

API_BASE_URL = "https://api.thenewshop.in"

# ---------------------------------------------------------------------------
# IMPORTANT:
#
# For production, these should ideally come from Databricks Secrets rather
# than being hardcoded in the notebook.
#
# Example:
#
# API_USERNAME = dbutils.secrets.get(
#     scope="thenewshop",
#     key="api-username"
# )
#
# API_PASSWORD = dbutils.secrets.get(
#     scope="thenewshop",
#     key="api-password"
# )
#
# Keep your existing credentials/configuration here if you already have
# them stored securely.
# ---------------------------------------------------------------------------

API_USERNAME = "nssomin"
API_PASSWORD = "nssomin"


# =============================================================================
# Source columns
# =============================================================================

COLS = [

    "grnId",
    "Date",
    "Time",

    "productName",
    "barcode",

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

    "vendor",
    "vendorGst",

    "invoiceNumber",

    "storeName",
    "brandName",
    "categoryName",

    "user",
]


# =============================================================================
# Numeric columns
# =============================================================================

NUMERIC_COLS = [

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
]


# =============================================================================
# Date formats accepted from API
# =============================================================================

DATE_FORMATS = [

    "%m/%d/%Y",
    "%d-%m-%Y",
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%m-%d-%Y",

]


# =============================================================================
# Decimal columns
# =============================================================================

DECIMAL_COLS = [

    "acess",
    "acessAmount",
    "amountNet",

    "cess",
    "cessAmount",

    "costPrice",

    "gst",
    "gstAmount",

    "sellingPrice",

    "totalCost",
    "totalTaxes",

]


# =============================================================================
# Helper — flexible date parser
# =============================================================================

def parse_date_flexible(
    val,
    from_date=None,
    to_date=None
):

    if val is None:
        return None

    s = str(val).strip()

    if s in (
        "",
        "Na",
        "nan",
        "NaT",
        "None",
    ):
        return None

    candidates = []

    for fmt in DATE_FORMATS:

        try:

            candidates.append(
                datetime.strptime(
                    s,
                    fmt
                ).date()
            )

        except ValueError:
            continue

    if not candidates:
        return None

    if len(candidates) == 1:
        return candidates[0]

    # -------------------------------------------------------------------------
    # If the date is ambiguous, use the requested processing window.
    # -------------------------------------------------------------------------

    if from_date and to_date:

        try:

            lo = datetime.strptime(
                from_date,
                "%Y-%m-%d"
            ).date()

            hi = datetime.strptime(
                to_date,
                "%Y-%m-%d"
            ).date()

            in_range = [
                d
                for d in candidates
                if lo <= d <= hi
            ]

            if in_range:
                return in_range[0]

        except ValueError:
            pass

    return candidates[0]


# =============================================================================
# Helper — flexible time parser
# =============================================================================

def parse_time_flexible(val):

    if val is None:
        return None

    s = str(val).strip()

    if s in (
        "",
        "Na",
        "nan",
        "NaT",
        "None",
    ):
        return None

    for fmt in (
        "%I:%M:%S %p",
        "%H:%M:%S",
        "%I:%M %p",
        "%H:%M",
    ):

        try:

            return datetime.strptime(
                s,
                fmt
            ).strftime(
                "%H:%M:%S"
            )

        except ValueError:
            continue

    return None


# =============================================================================
# Helper — barcode normalization
# =============================================================================

def clean_barcode(x):

    if not pd.notna(x):
        return None

    s = str(x).strip()

    if s in (
        "",
        "Na",
        "nan",
        "NaT",
        "None",
    ):
        return None

    try:

        return str(
            int(
                float(s)
            )
        )

    except (
        ValueError,
        OverflowError,
    ):

        return s


# =============================================================================
# Helper — BIGINT-safe conversion
# =============================================================================

def safe_to_bigint(
    series: pd.Series
) -> pd.Series:

    """
    Convert recievedQuantity to nullable Int64.

    Values outside BIGINT range are converted to NULL and logged.
    """

    numeric = pd.to_numeric(
        series,
        errors="coerce"
    )

    overflow_mask = (
        numeric.notna()
        &
        (
            (numeric > 9_223_372_036_854_775_807)
            |
            (numeric < -9_223_372_036_854_775_808)
        )
    )

    if overflow_mask.any():

        bad = series[
            overflow_mask
        ].tolist()

        print(
            f"  WARNING: "
            f"{overflow_mask.sum()} "
            f"recievedQuantity value(s) "
            f"overflow BIGINT and will be NULL: "
            f"{bad[:10]}"
        )

    coerced = numeric.where(
        ~overflow_mask,
        other=pd.NA
    )

    return coerced.astype("Int64")


# =============================================================================
# API CLIENT
# =============================================================================

class GRNClient:

    def __init__(self):

        self.base_url = API_BASE_URL
        self.username = API_USERNAME
        self.password = API_PASSWORD

        self.token = None

        self.session = requests.Session()


    # =========================================================================
    # Authentication
    # =========================================================================

    def authenticate(
        self,
        retries=3,
        delay=5
    ):

        self.token = None

        for attempt in range(
            retries
        ):

            print(
                f"  Authenticating "
                f"(attempt {attempt + 1}/{retries})..."
            )

            try:

                response = self.session.post(

                    f"{self.base_url}/login",

                    json={
                        "username": self.username,
                        "password": self.password,
                    },

                    timeout=10,
                )

            except requests.exceptions.RequestException as e:

                print(
                    f"    Network error: {e}"
                )

                if attempt < retries - 1:
                    time.sleep(delay)

                continue


            if response.status_code in (
                200,
                201,
            ):

                try:

                    payload = response.json()

                except Exception as e:

                    print(
                        f"  Authentication response "
                        f"was not valid JSON: {e}"
                    )

                    payload = {}


                self.token = payload.get(
                    "token"
                )

                if self.token:

                    print(
                        "  Authentication successful."
                    )

                    return True

                print(
                    "  Authentication response "
                    "did not contain a token."
                )

            else:

                print(
                    f"  Authentication HTTP "
                    f"{response.status_code}."
                )

                try:

                    print(
                        f"  Response: "
                        f"{response.text[:500]}"
                    )

                except Exception:
                    pass


            if attempt < retries - 1:
                time.sleep(delay)


        raise RuntimeError(
            "Authentication failed after "
            f"{retries} attempts."
        )


    # =========================================================================
    # Download GRN
    # =========================================================================

    def download(
        self,
        from_date: str,
        to_date: str
    ) -> pd.DataFrame:

        """
        Download GRN CSV for a specific date range.

        Re-authenticates automatically if the API returns HTTP 401.
        """

        if not self.token:
            self.authenticate()


        headers = {

            "accept": "*/*",

            "Authorization": self.token,

        }


        params = {

            "fromDate": from_date,
            "toDate": to_date,

        }


        print(
            f"  Downloading "
            f"{from_date} → {to_date} ..."
        )


        last_status = None


        for attempt in range(5):

            try:

                response = self.session.get(

                    f"{self.base_url}/GRN/GRNreportStore",

                    params=params,

                    headers=headers,

                    timeout=50,

                )

            except requests.exceptions.RequestException as e:

                print(
                    f"    Attempt {attempt + 1} "
                    f"network error: {e}"
                )

                if attempt < 4:
                    print(
                        "    Retrying in 10 seconds..."
                    )
                    time.sleep(10)

                continue


            last_status = response.status_code


            # -----------------------------------------------------------------
            # Token expired
            # -----------------------------------------------------------------

            if response.status_code == 401:

                print(
                    "    HTTP 401 — token expired. "
                    "Re-authenticating..."
                )

                self.authenticate()

                headers["Authorization"] = self.token

                continue


            # -----------------------------------------------------------------
            # Successful request
            # -----------------------------------------------------------------

            if response.status_code == 200:

                break


            print(
                f"    Attempt {attempt + 1}: "
                f"HTTP {response.status_code}"
            )

            try:

                print(
                    f"    Response: "
                    f"{response.text[:500]}"
                )

            except Exception:
                pass


            if attempt < 4:

                print(
                    "    Retrying in 10 seconds..."
                )

                time.sleep(10)


        else:

            raise RuntimeError(
                "GRN download failed after "
                f"5 attempts. "
                f"Last HTTP status: {last_status}"
            )


        # ---------------------------------------------------------------------
        # Decode response
        # ---------------------------------------------------------------------

        try:

            text = response.content.decode(
                "utf-8"
            )

        except UnicodeDecodeError as e:

            raise RuntimeError(
                f"GRN response decode error: {e}"
            )


        # ---------------------------------------------------------------------
        # API sometimes returns Base64 CSV.
        # ---------------------------------------------------------------------

        clean_response = re.sub(
            r"\s",
            "",
            text
        )


        try:

            csv_data = (
                base64
                .b64decode(
                    clean_response,
                    validate=True
                )
                .decode("utf-8")
            )

        except (
            binascii.Error,
            ValueError,
        ):

            csv_data = text


        # ---------------------------------------------------------------------
        # Parse CSV
        # ---------------------------------------------------------------------

        try:

            df = pd.read_csv(
                io.StringIO(csv_data)
            )

        except Exception as e:

            raise RuntimeError(
                f"GRN CSV parse error: {e}"
            )


        print(
            f"    {len(df):,} raw rows received."
        )


        if "Date" in df.columns:

            print(
                "    Date sample: "
                f"{df['Date'].head(3).tolist()}"
            )


        return df


# =============================================================================
# CLEANING
# =============================================================================

def clean(
    df: pd.DataFrame,
    from_date=None,
    to_date=None
) -> pd.DataFrame:

    # -------------------------------------------------------------------------
    # Keep only expected columns.
    # -------------------------------------------------------------------------

    available_columns = [
        c
        for c in COLS
        if c in df.columns
    ]

    df = df[
        available_columns
    ].copy()


    # -------------------------------------------------------------------------
    # Validate required columns
    # -------------------------------------------------------------------------

    required_columns = [

        "grnId",
        "Date",
        "barcode",
        "storeName",

    ]

    missing_columns = [

        c
        for c in required_columns
        if c not in df.columns

    ]


    if missing_columns:

        raise RuntimeError(
            "GRN API response is missing "
            f"required columns: {missing_columns}"
        )


    # -------------------------------------------------------------------------
    # Barcode
    # -------------------------------------------------------------------------

    df["barcode"] = (
        df["barcode"]
        .apply(clean_barcode)
    )


    # -------------------------------------------------------------------------
    # Date
    # -------------------------------------------------------------------------

    df["Date"] = (
        df["Date"]
        .apply(
            lambda v:
                parse_date_flexible(
                    v,
                    from_date=from_date,
                    to_date=to_date
                )
        )
    )


    null_date_count = (
        df["Date"]
        .isna()
        .sum()
    )


    if null_date_count:

        print(
            f"  WARNING: "
            f"{null_date_count:,} row(s) "
            f"have NULL Date after parsing "
            f"and will be dropped."
        )


    df = df[
        df["Date"].notna()
    ].copy()


    # -------------------------------------------------------------------------
    # Time
    # -------------------------------------------------------------------------

    if "Time" in df.columns:

        df["Time"] = (
            df["Time"]
            .apply(parse_time_flexible)
        )


    # -------------------------------------------------------------------------
    # GRN ID
    # -------------------------------------------------------------------------

    if "grnId" in df.columns:

        df["grnId"] = (
            df["grnId"]
            .astype(str)
            .str.strip()
        )


    # -------------------------------------------------------------------------
    # Numeric columns
    # -------------------------------------------------------------------------

    for column in NUMERIC_COLS:

        if column in df.columns:

            df[column] = pd.to_numeric(
                df[column],
                errors="coerce"
            )


    # -------------------------------------------------------------------------
    # Quantity
    # -------------------------------------------------------------------------

    if "recievedQuantity" in df.columns:

        df["recievedQuantity"] = (
            safe_to_bigint(
                df["recievedQuantity"]
            )
        )


    # -------------------------------------------------------------------------
    # Convert NaN / NaT to None
    # -------------------------------------------------------------------------

    df = df.where(
        pd.notna(df),
        None
    )


    return df


# =============================================================================
# ATOMIC DAILY SNAPSHOT REPLACEMENT
# =============================================================================
#
# THIS IS THE CRITICAL FIX.
#
# The old implementation used:
#
#     MERGE
#       ON target.Date = source.Date
#      AND target.grnId = source.grnId
#
# That is invalid because one GRN can contain multiple product lines.
#
# We therefore treat the API response as the authoritative snapshot for
# the requested business date.
#
# The complete Date partition is replaced atomically.
#
# =============================================================================

def atomic_snapshot_replace(
    sdf,
    dates: list
):

    """
    Atomically replace complete GRN date partition(s).

    This is the idempotent write strategy for the raw GRN snapshot.

    Example:

        Existing:
            2026-10-03 -> 12,000 rows

        New API snapshot:
            2026-10-03 -> 12,150 rows

        Result:
            2026-10-03 -> exactly 12,150 rows

    Re-running the same API snapshot produces the same result.
    """


    # =========================================================================
    # Basic validation
    # =========================================================================

    if not dates:

        print(
            "  No dates supplied — skipping write."
        )

        return 0


    # =========================================================================
    # Normalize date strings
    # =========================================================================

    dates = sorted(
        {
            str(d)
            for d in dates
        }
    )


    # =========================================================================
    # Step 1 — Write staging
    # =========================================================================

    print(
        "\n  [WRITE 1/5] "
        "Writing cleaned data to staging..."
    )


    (
        sdf.write
           .format("delta")
           .mode("overwrite")
           .option(
               "overwriteSchema",
               "false"
           )
           .saveAsTable(
               STAGING_TABLE
           )
    )


    staging_df = (
        spark.table(
            STAGING_TABLE
        )
    )


    staging_count = (
        staging_df.count()
    )


    print(
        f"  Staging rows: "
        f"{staging_count:,}"
    )


    # =========================================================================
    # Never delete an existing target partition if the API returned no rows.
    # =========================================================================

    if staging_count == 0:

        raise RuntimeError(
            "Staging is empty. "
            "Target was NOT modified because an empty API response "
            "must not erase an existing GRN date partition."
        )


    # =========================================================================
    # Step 2 — Validate staging schema
    # =========================================================================

    print(
        "\n  [WRITE 2/5] "
        "Validating staging schema..."
    )


    target_columns = (
        spark.table(
            FULL_TABLE
        ).columns
    )


    staging_columns = (
        staging_df.columns
    )


    missing_target_columns = [
        c
        for c in target_columns
        if c not in staging_columns
    ]


    unexpected_columns = [
        c
        for c in staging_columns
        if c not in target_columns
    ]


    if missing_target_columns:

        raise RuntimeError(
            "Staging schema validation failed. "
            f"Missing target columns: "
            f"{missing_target_columns}"
        )


    if unexpected_columns:

        raise RuntimeError(
            "Staging schema validation failed. "
            f"Unexpected columns: "
            f"{unexpected_columns}"
        )


    print(
        "  Staging schema validation passed."
    )


    # =========================================================================
    # Step 3 — Validate dates
    # =========================================================================

    print(
        "\n  [WRITE 3/5] "
        "Validating staging dates..."
    )


    invalid_date_count = (

        staging_df

        .filter(
            F.col("Date").isNull()
        )

        .count()

    )


    if invalid_date_count > 0:

        raise RuntimeError(
            "Staging contains "
            f"{invalid_date_count:,} rows "
            "with NULL Date."
        )


    # -------------------------------------------------------------------------
    # Get distinct staging dates
    # -------------------------------------------------------------------------

    staging_dates = [

        row["Date"].strftime(
            "%Y-%m-%d"
        )

        for row in (

            staging_df

            .select("Date")

            .distinct()

            .collect()

        )

    ]


    staging_dates = sorted(
        staging_dates
    )


    expected_dates = sorted(
        dates
    )


    # -------------------------------------------------------------------------
    # Ensure no unexpected dates exist.
    # -------------------------------------------------------------------------

    unexpected_dates = sorted(
        set(staging_dates)
        -
        set(expected_dates)
    )


    if unexpected_dates:

        raise RuntimeError(
            "CRITICAL: API response contains "
            f"unexpected dates: {unexpected_dates}. "
            f"Expected only: {expected_dates}. "
            "Target was NOT modified."
        )


    # -------------------------------------------------------------------------
    # Ensure every requested date exists.
    # -------------------------------------------------------------------------

    missing_dates = sorted(
        set(expected_dates)
        -
        set(staging_dates)
    )


    if missing_dates:

        raise RuntimeError(
            "CRITICAL: API response is missing "
            f"expected dates: {missing_dates}. "
            f"Received: {staging_dates}. "
            "Target was NOT modified."
        )


    print(
        f"  Expected dates: "
        f"{expected_dates}"
    )

    print(
        f"  Staging dates: "
        f"{staging_dates}"
    )

    print(
        "  Staging date validation passed."
    )


    # =========================================================================
    # Step 4 — Atomically replace Date partition
    # =========================================================================

    print(
        "\n  [WRITE 4/5] "
        f"Atomically replacing Date partition(s): "
        f"{expected_dates}"
    )


    # -------------------------------------------------------------------------
    # Build Delta replaceWhere predicate.
    #
    # Example:
    #
    #     Date = DATE '2026-10-03'
    #
    # For multiple dates:
    #
    #     Date = DATE '2026-10-01'
    #     OR Date = DATE '2026-10-02'
    # -------------------------------------------------------------------------

    replace_condition = " OR ".join(

        [
            f"Date = DATE '{d}'"
            for d in expected_dates
        ]

    )


    print(
        f"  replaceWhere: "
        f"{replace_condition}"
    )


    # -------------------------------------------------------------------------
    # CRITICAL:
    #
    # Delta performs this as a transactional overwrite.
    #
    # It replaces only rows matching replaceWhere.
    #
    # Existing rows for other dates remain untouched.
    #
    # If the write fails, Delta does not leave the table half-deleted.
    # -------------------------------------------------------------------------

    (
        staging_df

        .select(
            *target_columns
        )

        .write

        .format("delta")

        .mode("overwrite")

        .option(
            "replaceWhere",
            replace_condition
        )

        .saveAsTable(
            FULL_TABLE
        )
    )


    print(
        "  Atomic partition replacement completed."
    )


    # =========================================================================
    # Step 5 — Post-write validation
    # =========================================================================

    print(
        "\n  [WRITE 5/5] "
        "Running post-write validation..."
    )


    date_strs = ", ".join(

        [
            f"'{d}'"
            for d in expected_dates
        ]

    )


    # -------------------------------------------------------------------------
    # Count target rows
    # -------------------------------------------------------------------------

    written = (

        spark.sql(

            f"""
            SELECT COUNT(*) AS n
            FROM {FULL_TABLE}
            WHERE Date IN ({date_strs})
            """

        )

        .collect()[0]["n"]

    )


    print(
        f"  Staging rows : "
        f"{staging_count:,}"
    )

    print(
        f"  Target rows  : "
        f"{written:,}"
    )


    # -------------------------------------------------------------------------
    # Exact count must match.
    # -------------------------------------------------------------------------

    if written != staging_count:

        raise RuntimeError(
            "Post-write validation failed. "
            f"Staging contained {staging_count:,} rows "
            f"but target contains {written:,} rows "
            f"for dates {expected_dates}."
        )


    # -------------------------------------------------------------------------
    # Validate target dates
    # -------------------------------------------------------------------------

    target_dates = [

        row["Date"].strftime(
            "%Y-%m-%d"
        )

        for row in (

            spark.table(
                FULL_TABLE
            )

            .filter(
                F.col("Date").isin(
                    expected_dates
                )
            )

            .select("Date")

            .distinct()

            .collect()

        )

    ]


    target_dates = sorted(
        target_dates
    )


    if target_dates != expected_dates:

        raise RuntimeError(
            "Post-write date validation failed. "
            f"Expected {expected_dates}, "
            f"found {target_dates}."
        )


    # -------------------------------------------------------------------------
    # Validate NULL dates
    # -------------------------------------------------------------------------

    null_date_count = (

        spark.table(
            FULL_TABLE
        )

        .filter(
            F.col("Date").isin(
                expected_dates
            )
        )

        .filter(
            F.col("Date").isNull()
        )

        .count()

    )


    if null_date_count > 0:

        raise RuntimeError(
            "Post-write validation failed: "
            f"{null_date_count:,} rows have NULL Date."
        )


    print(
        "\n  Post-write validation PASSED."
    )

    print(
        f"  {written:,} rows confirmed "
        f"for {expected_dates}."
    )


    print(
        "  Target partition replacement "
        "is idempotent."
    )


    return written


# =============================================================================
# ORCHESTRATOR
# =============================================================================

def run_grn_load(
    from_date: str,
    to_date: str
):

    client = GRNClient()

    start = time.time()

    errors = []


    # =========================================================================
    # Validate input dates
    # =========================================================================

    try:

        lo = datetime.strptime(
            from_date,
            "%Y-%m-%d"
        ).date()

        hi = datetime.strptime(
            to_date,
            "%Y-%m-%d"
        ).date()

    except ValueError as e:

        raise ValueError(
            "Invalid date format. "
            "Expected YYYY-MM-DD."
        ) from e


    if lo > hi:

        raise ValueError(
            f"from_date {from_date} "
            f"is after to_date {to_date}."
        )


    # =========================================================================
    # Build date list
    # =========================================================================

    dates = []

    current = lo

    while current <= hi:

        dates.append(
            current.strftime(
                "%Y-%m-%d"
            )
        )

        current += timedelta(
            days=1
        )


    print(
        "\n"
        + "=" * 80
    )

    print(
        "GRN DAILY SNAPSHOT LOAD"
    )

    print(
        "=" * 80
    )

    print(
        f"From date : {from_date}"
    )

    print(
        f"To date   : {to_date}"
    )

    print(
        f"Dates     : {len(dates)}"
    )

    print(
        f"Target    : {FULL_TABLE}"
    )

    print(
        "=" * 80
    )


    total_written = 0

    successful_dates = 0


    # =========================================================================
    # Process one date at a time
    # =========================================================================

    for date_str in dates:

        print(
            "\n"
            + "-" * 80
        )

        print(
            f"PROCESSING GRN DATE: {date_str}"
        )

        print(
            "-" * 80
        )


        try:

            # =================================================================
            # Download
            # =================================================================

            pdf = client.download(

                from_date=date_str,

                to_date=date_str,

            )


            # =================================================================
            # Empty API response
            # =================================================================
            #
            # IMPORTANT:
            #
            # Do NOT replace the target partition with an empty DataFrame.
            #
            # An empty API response can mean:
            #
            #   - API problem
            #   - authentication issue
            #   - upstream delay
            #   - genuinely no GRN
            #
            # Therefore we preserve the existing target partition.
            # =================================================================

            if pdf.empty:

                print(
                    f"  No GRN data returned "
                    f"for {date_str}."
                )

                print(
                    "  Existing target partition "
                    "will NOT be modified."
                )

                continue


            print(
                "  Cleaning GRN data..."
            )


            # =================================================================
            # Cleaning
            # =================================================================

            pdf = clean(

                pdf,

                from_date=date_str,

                to_date=date_str,

            )


            print(
                f"  Rows after cleaning: "
                f"{len(pdf):,}"
            )


            if pdf.empty:

                raise RuntimeError(
                    f"All GRN rows were removed "
                    f"during cleaning for {date_str}. "
                    "Target partition was NOT modified."
                )


            # =================================================================
            # Validate that cleaned Pandas data contains only requested date
            # =================================================================

            cleaned_dates = sorted(

                {

                    d.strftime(
                        "%Y-%m-%d"
                    )

                    for d in pdf["Date"].dropna()

                }

            )


            if cleaned_dates != [date_str]:

                raise RuntimeError(
                    f"Cleaned GRN data for {date_str} "
                    f"contains unexpected dates: "
                    f"{cleaned_dates}. "
                    "Target partition was NOT modified."
                )


            print(
                f"  Date validation passed: "
                f"{date_str}"
            )


            # =================================================================
            # Create Spark DataFrame
            # =================================================================
            #
            # Spark infers the Pandas schema first.
            # Explicit casts below guarantee compatibility with Delta.
            # =================================================================

            sdf = spark.createDataFrame(
                pdf
            )


            # =================================================================
            # Date
            # =================================================================

            sdf = (

                sdf

                .withColumn(
                    "Date",
                    F.col("Date").cast("date")
                )

            )


            # =================================================================
            # Decimal columns
            # =================================================================

            for column_name in DECIMAL_COLS:

                if column_name in sdf.columns:

                    sdf = (

                        sdf

                        .withColumn(
                            column_name,
                            F.col(column_name)
                             .cast(
                                 "decimal(18,6)"
                             )
                        )

                    )


            # =================================================================
            # BIGINT quantity
            # =================================================================

            if "recievedQuantity" in sdf.columns:

                sdf = (

                    sdf

                    .withColumn(
                        "recievedQuantity",
                        F.col(
                            "recievedQuantity"
                        ).cast("bigint")
                    )

                )


            # =================================================================
            # Validate Spark DataFrame
            # =================================================================

            spark_null_dates = (

                sdf

                .filter(
                    F.col("Date").isNull()
                )

                .count()

            )


            if spark_null_dates > 0:

                raise RuntimeError(
                    f"{spark_null_dates:,} rows "
                    f"have NULL Date after Spark conversion."
                )


            spark_dates = [

                row["Date"].strftime(
                    "%Y-%m-%d"
                )

                for row in (

                    sdf

                    .select("Date")

                    .distinct()

                    .collect()

                )

            ]


            spark_dates = sorted(
                spark_dates
            )


            if spark_dates != [date_str]:

                raise RuntimeError(
                    f"Spark DataFrame date validation "
                    f"failed for {date_str}. "
                    f"Found: {spark_dates}"
                )


            # =================================================================
            # Write atomically
            # =================================================================

            written = atomic_snapshot_replace(

                sdf,

                [date_str]

            )


            total_written += written

            successful_dates += 1


            print(
                "\n"
                f"  SUCCESS: {date_str}"
            )

            print(
                f"  Rows written: "
                f"{written:,}"
            )


        except Exception as e:

            error_message = (
                f"{type(e).__name__}: {str(e)}"
            )


            full_traceback = (
                traceback.format_exc()
            )


            print(
                "\n"
                + "!" * 80
            )

            print(
                f"FAILED DATE: {date_str}"
            )

            print(
                error_message
            )

            print(
                full_traceback
            )

            print(
                "!" * 80
            )


            errors.append(
                (
                    date_str,
                    error_message
                )
            )


            # -------------------------------------------------------------
            # Continue processing other dates.
            # -------------------------------------------------------------

            continue


    # =========================================================================
    # Final summary
    # =========================================================================

    elapsed = (
        time.time()
        - start
    )


    print(
        "\n"
        + "=" * 80
    )

    print(
        "GRN LOAD COMPLETE"
    )

    print(
        "=" * 80
    )

    print(
        f"Requested dates : {len(dates)}"
    )

    print(
        f"Succeeded       : {successful_dates}"
    )

    print(
        f"Failed          : {len(errors)}"
    )

    print(
        f"Total rows      : {total_written:,}"
    )

    print(
        f"Elapsed         : {elapsed:.1f}s"
    )

    print(
        "=" * 80
    )


    # =========================================================================
    # Failed dates
    # =========================================================================

    if errors:

        print(
            "\nFAILED DATES:"
        )


        for failed_date, error in errors:

            print(
                f"  {failed_date} "
                f"→ {error}"
            )


        print(
            "\n"
            "The failed dates were NOT considered successfully loaded."
        )


        raise RuntimeError(

            f"GRN load completed with "
            f"{len(errors)} failed date(s): "
            f"{[d for d, _ in errors]}. "
            "See logs above."

        )


    print(
        "\n"
        "All requested dates loaded successfully."
    )

    print(
        "The load is idempotent: "
        "rerunning the same dates replaces their snapshots "
        "without accumulating duplicate rows."
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## CELL 3 — Widget / Scheduling Logic
# MAGIC
# MAGIC Supported modes:
# MAGIC
# MAGIC - `yesterday`
# MAGIC - `date`
# MAGIC - `backfill`
# MAGIC
# MAGIC Examples:
# MAGIC
# MAGIC `run_mode = yesterday`
# MAGIC
# MAGIC `run_mode = date`
# MAGIC `date = 2026-10-03`
# MAGIC
# MAGIC `run_mode = backfill`
# MAGIC `from_date = 2026-10-01`
# MAGIC `to_date = 2026-10-03`


# COMMAND ----------

def get_widget(
    name,
    default
):

    """
    Safely read a Databricks widget.

    Returns default only when the widget does not exist.

    Other exceptions are re-raised because they may represent real
    configuration/runtime problems.
    """

    try:

        value = dbutils.widgets.get(
            name
        )

        return (
            value
            if value.strip()
            else default
        )

    except Exception as e:

        message = str(e)

        if (
            "No input widget" in message
            or
            "InputWidgetNotDefined" in message
        ):

            return default

        raise


# =============================================================================
# Determine run mode
# =============================================================================

run_mode = get_widget(
    "run_mode",
    "yesterday"
)


# =============================================================================
# YESTERDAY
# =============================================================================

if run_mode == "yesterday":

    processing_date = (

        datetime.now()
        - timedelta(days=1)

    ).strftime(
        "%Y-%m-%d"
    )


    print(
        f"Run mode: yesterday"
    )

    print(
        f"Processing date: "
        f"{processing_date}"
    )


    run_grn_load(

        from_date=processing_date,

        to_date=processing_date,

    )


# =============================================================================
# SINGLE DATE
# =============================================================================

elif run_mode == "date":

    default_date = (

        datetime.now()
        - timedelta(days=1)

    ).strftime(
        "%Y-%m-%d"
    )


    processing_date = get_widget(
        "date",
        default_date
    )


    print(
        "Run mode: date"
    )

    print(
        f"Processing date: "
        f"{processing_date}"
    )


    run_grn_load(

        from_date=processing_date,

        to_date=processing_date,

    )


# =============================================================================
# BACKFILL
# =============================================================================

elif run_mode == "backfill":

    from_date = get_widget(
        "from_date",
        None
    )

    to_date = get_widget(
        "to_date",
        None
    )


    if not from_date or not to_date:

        raise ValueError(

            "backfill mode requires "
            "'from_date' and 'to_date' widgets. "

            "Set both widgets before running."

        )


    print(
        "Run mode: backfill"
    )

    print(
        f"From date: {from_date}"
    )

    print(
        f"To date: {to_date}"
    )


    run_grn_load(

        from_date=from_date,

        to_date=to_date,

    )


# =============================================================================
# INVALID MODE
# =============================================================================

else:

    raise ValueError(

        f"Unknown run_mode: '{run_mode}'. "

        "Valid values are: "
        "yesterday | date | backfill"

    )

# COMMAND ----------

