# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# ============================================================
# STORE MASTER + EMAIL UPDATE
# DATABRICKS
#
# PURPOSE
#   1. Find running stores from billing_silver
#   2. Fetch latest stores from Store API
#   3. Match running stores with API
#   4. Synchronize partner.csv with latest running stores
#   5. NEVER create duplicate running-store rows
#   6. Preserve existing partner information
#   7. Fetch /users
#   8. Update missing emails
#
# IMPORTANT API BEHAVIOR
#   - Login uses POST /login
#   - Store API uses RAW token in Authorization
#   - NO "Bearer"
#   - Store API pagination is ONE-BASED
#   - take=40
#   - sortField=addDate
#   - /users has no pagination parameters
# ============================================================


# ============================================================
# 1. IMPORTS
# ============================================================

import os
import json
import re
import time
import shutil
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd
import requests

from pyspark.sql import functions as F


# ============================================================
# 2. CONFIGURATION
# ============================================================

SCRIPT_VERSION = "DATABRICKS-STORE-MASTER-EMAIL-2026-10-04"

# ------------------------------------------------------------
# Databricks Silver
# ------------------------------------------------------------

SILVER_TABLE = "billing.sales_data.billing_silver"

LOOKBACK_DAYS = 7


# ------------------------------------------------------------
# Unity Catalog Volume
# ------------------------------------------------------------

VOLUME_DIR = "/Volumes/billing/sales_data/new_volume"

PARTNER_CSV = f"{VOLUME_DIR}/partner.csv"

PARTNER_MONTH_CSV = f"{VOLUME_DIR}/partner_month.csv"

OUTPUT_DIR = f"{VOLUME_DIR}/store_update"


# ------------------------------------------------------------
# Output files
# ------------------------------------------------------------

RUNNING_STORES_CSV = f"{OUTPUT_DIR}/running_stores.csv"

ALL_STORES_CSV = f"{OUTPUT_DIR}/all_api_stores.csv"

UNMATCHED_BILLING_STORES_CSV = (
    f"{OUTPUT_DIR}/unmatched_billing_stores.csv"
)

STORES_JSON_FILE = (
    f"{OUTPUT_DIR}/stores_response.json"
)

USERS_JSON_FILE = (
    f"{OUTPUT_DIR}/users_response.json"
)

COMBINED_JSON_FILE = (
    f"{OUTPUT_DIR}/store_users_with_emails.json"
)

COMBINED_CSV_FILE = (
    f"{OUTPUT_DIR}/store_users_with_emails.csv"
)

MISSING_STORES_CSV_FILE = (
    f"{OUTPUT_DIR}/missing_stores.csv"
)

PARTNER_SUMMARY_CSV_FILE = (
    f"{OUTPUT_DIR}/partner_update_summary.csv"
)


# ------------------------------------------------------------
# API
# ------------------------------------------------------------

BASE_URL = "https://api.thenewshop.in"

LOGIN_URL = f"{BASE_URL}/login"

STORE_URL = f"{BASE_URL}/store"

USERS_URL = f"{BASE_URL}/users"


# ------------------------------------------------------------
# Credentials
# ------------------------------------------------------------

USERNAME = os.getenv(
    "NEWSHOP_USERNAME",
    "nssomin",
)

PASSWORD = os.getenv(
    "NEWSHOP_PASSWORD",
    "nssomin",
)


# ------------------------------------------------------------
# API settings
# ------------------------------------------------------------

PAGE_SIZE = 40

MAX_PAGES = 200

REQUEST_TIMEOUT = 60

MAX_RETRIES = 3

RETRY_DELAY = 3


# ------------------------------------------------------------
# IMPORTANT
#
# Original Store API expects RAW token.
#
# DO NOT change this to:
#
# Authorization: Bearer <token>
# ------------------------------------------------------------

AUTH_PREFIX = ""


# ------------------------------------------------------------
# Partner CSV structure
# ------------------------------------------------------------

PARTNER_ID_COLUMN = "store"

PARTNER_NAME_COLUMNS = (
    "storeName",
    "storename",
    "store_name",
)

ATOMIC_WRITE_ENCODING = "utf-8-sig"


# ============================================================
# 3. CREATE OUTPUT DIRECTORY
# ============================================================

os.makedirs(
    OUTPUT_DIR,
    exist_ok=True,
)


# ============================================================
# 4. HELPER FUNCTIONS
# ============================================================

def clean_string(value: Any) -> str:
    """
    Convert a value to a clean string.

    Empty / NaN / None -> ""
    """
    if value is None:
        return ""

    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass

    return str(value).strip()


def normalize_store_name(value: Any) -> str:
    """
    Normalize store names for matching.

    Example:

        "Delhi   Karol Bagh"
        " Delhi Karol Bagh "

    becomes:

        "delhi karol bagh"
    """
    value = clean_string(value)

    if not value:
        return ""

    return " ".join(
        value.lower().split()
    )


def normalize_store_name_email(value: Any) -> str:
    """
    Stronger normalization used for email/store-user matching.
    """
    value = clean_string(value)

    if not value:
        return ""

    text = value.lower()

    text = text.replace(
        "&",
        "and",
    )

    text = re.sub(
        r"[^a-z0-9]+",
        " ",
        text,
    )

    text = re.sub(
        r"\s+",
        " ",
        text,
    ).strip()

    return text


def normalize_store_id(value: Any) -> str:
    """
    Normalize Store API IDs / CSV IDs.

    Example:

        226
        "226"
        "226.0"

    all become:

        "226"
    """
    value = clean_string(value)

    if not value:
        return ""

    if value.endswith(".0"):
        value = value[:-2]

    return value


def get_first_value(
    record: Dict[str, Any],
    fields: Iterable[str],
) -> Any:
    """
    Return the first non-empty field.
    """
    for field in fields:

        if field not in record:
            continue

        value = record.get(field)

        if value is None:
            continue

        if isinstance(
            value,
            str,
        ) and not value.strip():
            continue

        return value

    return None


def first_non_empty(
    values: Iterable[Any],
) -> str:
    """
    Return the first non-empty value.
    """
    for value in values:

        value = clean_string(value)

        if value:
            return value

    return ""


# ============================================================
# 5. BACKUP FUNCTION
# ============================================================

def backup_file(
    file_path: str,
) -> Optional[str]:
    """
    Create timestamped backup before modifying a CSV.
    """

    if not os.path.exists(file_path):
        return None

    path = Path(file_path)

    timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S"
    )

    backup_path = path.with_name(
        f"{path.stem}_backup_{timestamp}{path.suffix}"
    )

    shutil.copy2(
        path,
        backup_path,
    )

    print(
        f"Backup created: {backup_path}"
    )

    return str(backup_path)


# ============================================================
# 6. ATOMIC CSV WRITE
# ============================================================

def atomic_write_csv(
    dataframe: pd.DataFrame,
    file_path: str,
) -> None:
    """
    Write CSV through temporary file and replace atomically.
    """

    temporary_path = (
        f"{file_path}.tmp"
    )

    try:

        dataframe.to_csv(
            temporary_path,
            index=False,
            encoding=ATOMIC_WRITE_ENCODING,
        )

        os.replace(
            temporary_path,
            file_path,
        )

    finally:

        if os.path.exists(
            temporary_path
        ):
            os.remove(
                temporary_path
            )


# ============================================================
# 7. FIND PARTNER NAME COLUMN
# ============================================================

def find_partner_name_column(
    dataframe: pd.DataFrame,
) -> str:

    for column in PARTNER_NAME_COLUMNS:

        if column in dataframe.columns:
            return column

    print(
        "WARNING: No store name column found."
    )

    print(
        "Creating storeName."
    )

    dataframe["storeName"] = ""

    return "storeName"


# ============================================================
# 8. IDENTIFY RUNNING STORES
# ============================================================

print("=" * 100)
print("STORE MASTER + EMAIL UPDATE")
print("=" * 100)

print(
    f"Pipeline Run ID : "
    f"{datetime.now().strftime('%Y%m%d_%H%M%S')}"
)

print(
    f"Silver Table    : {SILVER_TABLE}"
)

print(
    f"Partner CSV     : {PARTNER_CSV}"
)

print(
    f"Lookback Days   : {LOOKBACK_DAYS}"
)

print("=" * 100)
print("STEP 1 — IDENTIFY RUNNING STORES")
print("=" * 100)


running_store_df = (
    spark.table(
        SILVER_TABLE
    )

    .filter(
        F.col("orderDate").isNotNull()
    )

    .filter(
        F.to_date("orderDate")
        >= F.date_sub(
            F.current_date(),
            LOOKBACK_DAYS,
        )
    )

    .filter(
        F.col("storeName").isNotNull()
    )

    .filter(
        F.trim(
            F.col("storeName")
        ) != ""
    )

    .select(
        F.trim(
            F.col("storeName")
        ).alias("storeName")
    )

    .distinct()
)


running_store_rows = (
    running_store_df.collect()
)


billing_store_names: Set[str] = set()


for row in running_store_rows:

    store_name = row["storeName"]

    normalized_name = normalize_store_name(
        store_name
    )

    if normalized_name:

        billing_store_names.add(
            normalized_name
        )


print(
    f"Running stores found in Silver: "
    f"{len(billing_store_names)}"
)


if not billing_store_names:

    raise RuntimeError(
        "No running stores found in billing_silver. "
        "partner.csv will NOT be modified."
    )


# ============================================================
# 9. HTTP SESSION
# ============================================================

def create_session() -> requests.Session:

    session = requests.Session()

    session.headers.update(
        {
            "accept": "*/*",
            "User-Agent": "AzureStoreUpdater/2.0",
        }
    )

    return session


# ============================================================
# 10. LOGIN
# ============================================================

def login(
    session: requests.Session,
) -> str:

    payload = {
        "username": USERNAME,
        "password": PASSWORD,
    }

    print(
        f"POST {LOGIN_URL}"
    )

    response = session.post(
        LOGIN_URL,
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )

    print(
        f"Login -> {response.status_code}"
    )

    if not response.ok:

        print(
            "Login response:"
        )

        print(
            response.text[:5000]
        )

    response.raise_for_status()

    body = response.json()

    token = None

    if isinstance(
        body,
        dict,
    ):

        token = (
            body.get("token")
            or body.get("access_token")
            or body.get("accessToken")
        )

        nested_data = body.get(
            "data"
        )

        if isinstance(
            nested_data,
            dict,
        ):

            token = (
                token
                or nested_data.get("token")
                or nested_data.get("access_token")
                or nested_data.get("accessToken")
            )

    if not token:

        raise RuntimeError(
            "Login succeeded, but no authentication "
            f"token was found in response: {body}"
        )

    print(
        "Store API login successful"
    )

    return str(token)


# ============================================================
# 11. AUTH HEADERS
# ============================================================

def build_auth_headers(
    token: str,
) -> Dict[str, str]:

    if AUTH_PREFIX:

        auth_value = (
            f"{AUTH_PREFIX} {token}"
        )

    else:

        auth_value = token

    return {
        "Authorization": auth_value,
        "accept": "*/*",
        "Content-Type": "application/json",
    }


# ============================================================
# 12. REQUEST WITH RETRIES
# ============================================================

def request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    *,
    headers: Optional[
        Dict[str, str]
    ] = None,
    params: Optional[
        Dict[str, Any]
    ] = None,
) -> requests.Response:

    last_exception = None

    last_response = None

    for attempt in range(
        1,
        MAX_RETRIES + 1,
    ):

        try:

            response = session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            last_response = response

            print(
                f"{method.upper()} "
                f"{response.url} "
                f"-> {response.status_code}"
            )

            if response.status_code == 200:

                return response

            retryable = (
                response.status_code == 429
                or response.status_code >= 500
            )

            print(
                f"Request failed "
                f"{attempt}/{MAX_RETRIES}: "
                f"{response.status_code} "
                f"{response.text[:1000]}"
            )

            if not retryable:

                response.raise_for_status()

            if attempt < MAX_RETRIES:

                time.sleep(
                    RETRY_DELAY * attempt
                )

        except requests.RequestException as exc:

            last_exception = exc

            print(
                f"Request exception "
                f"{attempt}/{MAX_RETRIES}: "
                f"{exc}"
            )

            if attempt < MAX_RETRIES:

                time.sleep(
                    RETRY_DELAY * attempt
                )

    if last_response is not None:

        last_response.raise_for_status()

    if last_exception is not None:

        raise last_exception

    raise RuntimeError(
        f"Request failed: {method} {url}"
    )


# ============================================================
# 13. API STORE RECORD NORMALIZATION
# ============================================================

def normalize_api_store(
    record: Dict[str, Any],
) -> Optional[Dict[str, Any]]:

    store_id = get_first_value(
        record,
        [
            "id",
            "storeId",
            "store_id",
        ],
    )

    store_name = get_first_value(
        record,
        [
            "storename",
            "storeName",
            "store_name",
            "name",
        ],
    )

    store_id = normalize_store_id(
        store_id
    )

    store_name = clean_string(
        store_name
    )

    if not store_id or not store_name:

        return None

    return {
        "store_id": store_id,
        "store_name": store_name,
        "normalized_store_name":
            normalize_store_name(
                store_name
            ),
    }


# ============================================================
# 14. FETCH ALL STORE API RECORDS
# ============================================================

def fetch_all_api_stores(
    session: requests.Session,
    token: str,
    from_date: str,
    to_date: str,
) -> List[Dict[str, Any]]:

    headers = build_auth_headers(
        token
    )

    all_records = []

    unique_by_id = {}

    total_count = None

    print("=" * 100)
    print("STEP 2 — FETCH STORE API")
    print("=" * 100)

    for page_number in range(
        1,
        MAX_PAGES + 1,
    ):

        params = {
            "order": "ASC",
            "offset": page_number,
            "take": PAGE_SIZE,
            "fromDate": from_date,
            "toDate": to_date,
            "sortField": "addDate",
        }

        try:

            response = request_with_retries(
                session,
                "GET",
                STORE_URL,
                headers=headers,
                params=params,
            )

        except requests.HTTPError:

            # ------------------------------------------------
            # EXACT FALLBACK:
            #
            # Retry without sortField=addDate
            # ------------------------------------------------

            print(
                "Store API request with "
                "sortField=addDate failed."
            )

            print(
                "Retrying same page without sortField."
            )

            params_without_sort = {
                "order": "ASC",
                "offset": page_number,
                "take": PAGE_SIZE,
                "fromDate": from_date,
                "toDate": to_date,
            }

            response = request_with_retries(
                session,
                "GET",
                STORE_URL,
                headers=headers,
                params=params_without_sort,
            )

        body = response.json()

        records = []

        if isinstance(
            body,
            list,
        ):

            records = body

        elif isinstance(
            body,
            dict,
        ):

            for key in (
                "data",
                "items",
                "stores",
                "results",
            ):

                value = body.get(
                    key
                )

                if isinstance(
                    value,
                    list,
                ):

                    records = value

                    break

            if total_count is None:

                for key in (
                    "total",
                    "totalCount",
                    "count",
                    "totalRecords",
                ):

                    value = body.get(
                        key
                    )

                    if value is not None:

                        try:

                            total_count = int(
                                value
                            )

                        except Exception:
                            pass

                        break

        print(
            f"Page {page_number} returned "
            f"{len(records)} stores"
            + (
                f" | total={total_count}"
                if total_count is not None
                else ""
            )
        )

        if not records:

            print(
                "No more records."
            )

            break

        for record in records:

            if not isinstance(
                record,
                dict,
            ):
                continue

            all_records.append(
                record
            )

            normalized = normalize_api_store(
                record
            )

            if normalized:

                unique_by_id[
                    normalized["store_id"]
                ] = normalized

        if (
            total_count is not None
            and len(unique_by_id) >= total_count
        ):

            print(
                f"Reached API total count "
                f"of {total_count}"
            )

            break

        if len(records) < PAGE_SIZE:

            print(
                "Short page received. "
                "Pagination complete."
            )

            break

    unique_stores = list(
        unique_by_id.values()
    )

    print(
        f"Fetched {len(all_records)} total API records "
        f"and {len(unique_stores)} unique stores"
    )

    return unique_stores


# ============================================================
# 15. DATE RANGE
# ============================================================

def get_api_date_range(
    lookback_days: int = 7,
) -> Tuple[str, str]:

    today = date.today()

    from_date = (
        today
        - timedelta(
            days=lookback_days
        )
    )

    return (
        from_date.strftime(
            "%Y-%m-%d"
        ),
        today.strftime(
            "%Y-%m-%d"
        ),
    )


# ============================================================
# 16. MATCH RUNNING STORES WITH API
# ============================================================

def match_running_stores(
    api_stores: List[Dict[str, Any]],
    billing_store_names: Set[str],
) -> Tuple[
    List[Dict[str, Any]],
    Set[str],
]:

    print("=" * 100)
    print("STEP 3 — MATCH RUNNING STORES")
    print("=" * 100)

    api_by_name = {}

    for store in api_stores:

        normalized_name = (
            store["normalized_store_name"]
        )

        if normalized_name:

            api_by_name[
                normalized_name
            ] = store

    matched_stores = []

    unmatched_names = set()

    for billing_name in sorted(
        billing_store_names
    ):

        api_store = api_by_name.get(
            billing_name
        )

        if api_store is None:

            unmatched_names.add(
                billing_name
            )

            continue

        matched_stores.append(
            {
                "storeId":
                    api_store["store_id"],

                "storeName":
                    api_store["store_name"],

                "normalizedStoreName":
                    billing_name,
            }
        )

    matched_stores.sort(
        key=lambda item:
            normalize_store_name(
                item["storeName"]
            )
    )

    print(
        f"Matched {len(matched_stores)} "
        f"of {len(billing_store_names)} "
        f"running stores with Store API"
    )

    if unmatched_names:

        print(
            f"WARNING: "
            f"{len(unmatched_names)} "
            f"running stores could not be matched."
        )

    else:

        print(
            "All running stores matched successfully."
        )

    return (
        matched_stores,
        unmatched_names,
    )


# ============================================================
# 17. SAVE API STORES
# ============================================================

def save_api_stores(
    api_stores: List[Dict[str, Any]],
) -> None:

    dataframe = pd.DataFrame(
        api_stores
    )

    dataframe.to_csv(
        ALL_STORES_CSV,
        index=False,
        encoding="utf-8",
    )

    print(
        f"Saved all API stores: "
        f"{ALL_STORES_CSV}"
    )


# ============================================================
# 18. SAVE RUNNING STORES
# ============================================================

def save_running_stores(
    matched_stores: List[Dict[str, Any]],
) -> None:

    dataframe = pd.DataFrame(
        matched_stores,
        columns=[
            "storeId",
            "storeName",
            "normalizedStoreName",
        ],
    )

    dataframe.to_csv(
        RUNNING_STORES_CSV,
        index=False,
        encoding="utf-8",
    )

    print(
        f"Saved running stores: "
        f"{RUNNING_STORES_CSV}"
    )


# ============================================================
# 19. SAVE UNMATCHED STORES
# ============================================================

def save_unmatched_billing_stores(
    unmatched_names: Set[str],
) -> None:

    dataframe = pd.DataFrame(
        sorted(
            unmatched_names
        ),
        columns=[
            "normalizedStoreName"
        ],
    )

    dataframe.to_csv(
        UNMATCHED_BILLING_STORES_CSV,
        index=False,
        encoding="utf-8",
    )

    print(
        f"Saved unmatched stores: "
        f"{UNMATCHED_BILLING_STORES_CSV}"
    )


# ============================================================
# 20. UPDATE PARTNER CSV
#
# THIS IS THE IMPORTANT FIX.
#
# Behavior:
#
#   Existing running store:
#       Match by STORE ID first.
#
#       Example:
#
#       partner.csv:
#           226 | Sonipat GT Road
#
#       API:
#           226 | Sonipat TDI City
#
#       RESULT:
#           226 | Sonipat TDI City
#
#       NOT:
#           226 | Sonipat GT Road
#           226 | Sonipat TDI City
#
#
#   New running store:
#       Append once.
#
#   Non-running store:
#       Preserve.
#
#   Duplicate rows:
#       Consolidate.
#
#   Existing email / wa_number / pdf_link / other fields:
#       Preserve.
# ============================================================

def update_partner_csv(
    matched_stores: List[Dict[str, Any]],
    partner_csv_path: str,
    dry_run: bool = False,
) -> Dict[str, Any]:

    print()
    print(
        f"Processing: {partner_csv_path}"
    )

    if not os.path.exists(
        partner_csv_path
    ):

        print(
            f"WARNING: File does not exist: "
            f"{partner_csv_path}"
        )

        return {
            "skipped": True,
            "reason": "file_not_found",
        }

    dataframe = pd.read_csv(
        partner_csv_path,
        dtype=str,
        keep_default_na=False,
    )

    if PARTNER_ID_COLUMN not in dataframe.columns:

        raise KeyError(
            f"Required column "
            f"'{PARTNER_ID_COLUMN}' "
            f"is missing from "
            f"{partner_csv_path}."
        )

    name_column = (
        find_partner_name_column(
            dataframe
        )
    )

    # --------------------------------------------------------
    # Ensure important columns exist.
    # --------------------------------------------------------

    for column in (
        "email",
        "wa_number",
    ):

        if column not in dataframe.columns:

            dataframe[column] = ""


    # --------------------------------------------------------
    # Normalize existing partner rows.
    # --------------------------------------------------------

    dataframe[
        "_normalized_id"
    ] = dataframe[
        PARTNER_ID_COLUMN
    ].apply(
        normalize_store_id
    )

    dataframe[
        "_normalized_name"
    ] = dataframe[
        name_column
    ].apply(
        normalize_store_name
    )


    # --------------------------------------------------------
    # Build latest running-store maps.
    #
    # ID is the PRIMARY identity for an ACTIVE store.
    # Name is the fallback.
    # --------------------------------------------------------

    running_by_id = {}

    running_by_name = {}

    for store in matched_stores:

        store_id = normalize_store_id(
            store.get("storeId")
        )

        store_name = clean_string(
            store.get("storeName")
        )

        normalized_name = (
            normalize_store_name(
                store_name
            )
        )

        if not store_id or not store_name:

            print(
                "WARNING: Invalid running store "
                f"skipped: {store}"
            )

            continue

        record = {
            "store_id": store_id,
            "store_name": store_name,
            "normalized_name":
                normalized_name,
        }

        running_by_id[
            store_id
        ] = record

        if normalized_name:

            running_by_name[
                normalized_name
            ] = record


    # --------------------------------------------------------
    # Counters
    # --------------------------------------------------------

    updated_rows = 0

    renamed_rows = 0

    added_rows = 0

    unchanged_rows = 0

    removed_duplicate_rows = 0

    unmatched_partner_rows = 0


    # --------------------------------------------------------
    # Track which active API store IDs already have a row.
    # --------------------------------------------------------

    processed_running_ids = set()

    rows_to_remove = set()


    # ========================================================
    # UPDATE EXISTING ROWS
    # ========================================================

    for index in dataframe.index:

        current_id = normalize_store_id(
            dataframe.at[
                index,
                PARTNER_ID_COLUMN,
            ]
        )

        current_name = clean_string(
            dataframe.at[
                index,
                name_column,
            ]
        )

        current_normalized_name = (
            normalize_store_name(
                current_name
            )
        )

        running_store = None

        # ----------------------------------------------------
        # FIRST: match by store ID.
        #
        # This is what fixes renamed stores.
        # ----------------------------------------------------

        if current_id:

            running_store = (
                running_by_id.get(
                    current_id
                )
            )

        # ----------------------------------------------------
        # SECOND: fallback to normalized name.
        # ----------------------------------------------------

        if (
            running_store is None
            and current_normalized_name
        ):

            running_store = (
                running_by_name.get(
                    current_normalized_name
                )
            )

        # ----------------------------------------------------
        # This partner row is not currently running.
        # Preserve it.
        # ----------------------------------------------------

        if running_store is None:

            unmatched_partner_rows += 1

            continue


        latest_id = (
            running_store[
                "store_id"
            ]
        )

        latest_name = (
            running_store[
                "store_name"
            ]
        )

        latest_normalized_name = (
            running_store[
                "normalized_name"
            ]
        )


        # ----------------------------------------------------
        # If this active store has already been processed,
        # this is a duplicate active row.
        #
        # Remove it.
        # ----------------------------------------------------

        if (
            latest_id
            in processed_running_ids
        ):

            rows_to_remove.add(
                index
            )

            removed_duplicate_rows += 1

            print(
                "Removing duplicate running "
                f"store row: "
                f"ID={latest_id} | "
                f"name='{current_name}'"
            )

            continue


        processed_running_ids.add(
            latest_id
        )


        # ----------------------------------------------------
        # Update store ID if necessary.
        # ----------------------------------------------------

        if current_id != latest_id:

            dataframe.at[
                index,
                PARTNER_ID_COLUMN,
            ] = latest_id

            updated_rows += 1


        # ----------------------------------------------------
        # CRITICAL:
        #
        # Update store name when API name changed.
        # ----------------------------------------------------

        if (
            current_normalized_name
            != latest_normalized_name
        ):

            print(
                "Updating active store: "
                f"ID={latest_id} | "
                f"'{current_name}' -> "
                f"'{latest_name}'"
            )

            dataframe.at[
                index,
                name_column,
            ] = latest_name

            dataframe.at[
                index,
                "_normalized_name",
            ] = latest_normalized_name

            updated_rows += 1

            renamed_rows += 1

        elif not current_name:

            dataframe.at[
                index,
                name_column,
            ] = latest_name

            dataframe.at[
                index,
                "_normalized_name",
            ] = latest_normalized_name

            updated_rows += 1

        else:

            unchanged_rows += 1


    # ========================================================
    # REMOVE DUPLICATE ACTIVE ROWS
    # ========================================================

    if rows_to_remove:

        dataframe = dataframe.drop(
            index=list(
                rows_to_remove
            )
        ).copy()

        dataframe.reset_index(
            drop=True,
            inplace=True,
        )


    # ========================================================
    # ADD MISSING RUNNING STORES
    # ========================================================

    existing_running_ids = set(
        dataframe[
            PARTNER_ID_COLUMN
        ].apply(
            normalize_store_id
        )
    )

    for store_id in sorted(
        running_by_id.keys()
    ):

        if store_id in existing_running_ids:

            continue

        store = running_by_id[
            store_id
        ]

        new_row = {
            column: ""
            for column in dataframe.columns
            if not column.startswith("_")
        }

        new_row[
            PARTNER_ID_COLUMN
        ] = store_id

        new_row[
            name_column
        ] = store[
            "store_name"
        ]

        dataframe = pd.concat(
            [
                dataframe,
                pd.DataFrame(
                    [new_row],
                    columns=[
                        column
                        for column
                        in dataframe.columns
                        if not column.startswith("_")
                    ],
                ),
            ],
            ignore_index=True,
        )

        existing_running_ids.add(
            store_id
        )

        added_rows += 1

        print(
            "Adding missing running store: "
            f"ID={store_id} | "
            f"name='{store['store_name']}'"
        )


    # ========================================================
    # REMOVE INTERNAL COLUMNS
    # ========================================================

    internal_columns = [
        column
        for column in dataframe.columns
        if column.startswith("_")
    ]

    if internal_columns:

        dataframe = dataframe.drop(
            columns=internal_columns
        )


    # ========================================================
    # FINAL CANONICAL NORMALIZATION
    #
    # Identity:
    #
    #     store ID
    #     +
    #     normalized store name
    #
    # Exact duplicate identities are consolidated.
    # ========================================================

    dataframe[
        "_normalized_id"
    ] = dataframe[
        PARTNER_ID_COLUMN
    ].apply(
        normalize_store_id
    )

    dataframe[
        "_normalized_name"
    ] = dataframe[
        name_column
    ].apply(
        normalize_store_name
    )


    valid_identity_mask = (
        dataframe[
            "_normalized_id"
        ].ne("")
        &
        dataframe[
            "_normalized_name"
        ].ne("")
    )


    valid_rows = dataframe[
        valid_identity_mask
    ].copy()

    invalid_rows = dataframe[
        ~valid_identity_mask
    ].copy()


    # --------------------------------------------------------
    # Consolidate duplicate canonical identities.
    #
    # Preserve the first row and fill empty fields from
    # duplicate rows.
    # --------------------------------------------------------

    consolidated_rows = []

    duplicate_groups_removed = 0

    for (
        identity,
        group,
    ) in valid_rows.groupby(
        [
            "_normalized_id",
            "_normalized_name",
        ],
        sort=False,
        dropna=False,
    ):

        if len(group) > 1:

            duplicate_groups_removed += (
                len(group) - 1
            )

        consolidated = (
            group.iloc[0].copy()
        )

        # ----------------------------------------------------
        # Preserve useful values from duplicate rows.
        # ----------------------------------------------------

        for column in dataframe.columns:

            if column.startswith("_"):
                continue

            current_value = clean_string(
                consolidated[column]
            )

            if current_value:
                continue

            for value in group[
                column
            ].tolist():

                value = clean_string(
                    value
                )

                if value:

                    consolidated[
                        column
                    ] = value

                    break

        consolidated_rows.append(
            consolidated
        )


    if duplicate_groups_removed:

        removed_duplicate_rows += (
            duplicate_groups_removed
        )


    if consolidated_rows:

        consolidated_df = pd.DataFrame(
            consolidated_rows
        )

    else:

        consolidated_df = pd.DataFrame(
            columns=valid_rows.columns
        )


    dataframe = pd.concat(
        [
            consolidated_df,
            invalid_rows,
        ],
        ignore_index=True,
    )


    # ========================================================
    # FINAL VALIDATION
    # ========================================================

    validation = dataframe.copy()

    validation[
        "_validation_id"
    ] = validation[
        PARTNER_ID_COLUMN
    ].apply(
        normalize_store_id
    )

    validation[
        "_validation_name"
    ] = validation[
        name_column
    ].apply(
        normalize_store_name
    )


    duplicate_identity = (
        validation[
            (
                validation[
                    "_validation_id"
                ] != ""
            )
            &
            (
                validation[
                    "_validation_name"
                ] != ""
            )
        ]

        .groupby(
            [
                "_validation_id",
                "_validation_name",
            ]
        )

        .size()

        .reset_index(
            name="count"
        )

        .query(
            "count > 1"
        )
    )


    if len(
        duplicate_identity
    ) > 0:

        print(
            "DUPLICATE IDENTITIES FOUND:"
        )

        display(
            duplicate_identity
        )

        raise RuntimeError(
            "partner.csv still contains "
            "duplicate store_id + "
            "normalized_store_name identities."
        )


    # --------------------------------------------------------
    # Remove validation columns.
    # --------------------------------------------------------

    dataframe = dataframe.drop(
        columns=[
            "_validation_id",
            "_validation_name",
        ],
        errors="ignore",
    )


    # ========================================================
    # FINAL SUMMARY
    # ========================================================

    summary = {
        "updated_rows":
            updated_rows,

        "renamed_rows":
            renamed_rows,

        "added_rows":
            added_rows,

        "unchanged_rows":
            unchanged_rows,

        "removed_duplicate_rows":
            removed_duplicate_rows,

        "unmatched_partner_rows":
            unmatched_partner_rows,

        "total_partner_rows":
            len(dataframe),

        "active_api_stores":
            len(running_by_id),
    }


    print()
    print(
        "Partner CSV update summary:"
    )

    print(
        summary
    )


    # ========================================================
    # DRY RUN
    # ========================================================

    if dry_run:

        print(
            "DRY RUN enabled."
        )

        print(
            "partner.csv was NOT modified."
        )

        return summary


    # ========================================================
    # BACKUP
    # ========================================================

    backup_path = backup_file(
        partner_csv_path
    )


    # ========================================================
    # ATOMIC WRITE
    # ========================================================

    atomic_write_csv(
        dataframe,
        partner_csv_path,
    )


    print(
        f"Partner CSV updated successfully: "
        f"{partner_csv_path}"
    )


    summary[
        "backup"
    ] = backup_path

    return summary


# ============================================================
# 21. FETCH USERS
# ============================================================

def fetch_all_users(
    session: requests.Session,
    token: str,
) -> List[Dict[str, Any]]:

    print("=" * 100)
    print("STEP 5 — FETCH USERS")
    print("=" * 100)

    headers = build_auth_headers(
        token
    )

    print(
        f"GET {USERS_URL}"
    )

    response = session.get(
        USERS_URL,
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )

    print(
        f"GET {USERS_URL} "
        f"-> {response.status_code}"
    )

    response.raise_for_status()

    body = response.json()

    users = []

    if isinstance(
        body,
        list,
    ):

        users = body

    elif isinstance(
        body,
        dict,
    ):

        for key in (
            "data",
            "items",
            "users",
            "results",
        ):

            value = body.get(
                key
            )

            if isinstance(
                value,
                list,
            ):

                users = value

                break

    print(
        f"Total users fetched: "
        f"{len(users)}"
    )

    # --------------------------------------------------------
    # Save raw response.
    # --------------------------------------------------------

    with open(
        USERS_JSON_FILE,
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            body,
            file,
            indent=2,
            ensure_ascii=False,
            default=str,
        )

    return users


# ============================================================
# 22. MATCH STORE USERS WITH /USERS
#
# The /users response contains store relationships.
#
# We intentionally support several possible field names
# because the API response structure can contain:
#
#   storeId
#   storeID
#   store
#   store_id
#
# and:
#
#   email
#   userEmail
#   emailAddress
#
# ============================================================

def combine_store_users(
    matched_stores: List[Dict[str, Any]],
    users: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:

    print("=" * 100)
    print(
        "STEP 6 — MATCH STORE USERS WITH /USERS"
    )
    print("=" * 100)

    matched_by_id = {}

    for store in matched_stores:

        store_id = normalize_store_id(
            store.get("storeId")
        )

        if store_id:

            matched_by_id[
                store_id
            ] = store


    combined_records = []


    for user in users:

        if not isinstance(
            user,
            dict,
        ):
            continue

        store_id = get_first_value(
            user,
            [
                "storeId",
                "storeID",
                "store_id",
                "store",
            ],
        )

        store_id = normalize_store_id(
            store_id
        )

        if not store_id:

            continue

        store = matched_by_id.get(
            store_id
        )

        if store is None:

            continue

        email = get_first_value(
            user,
            [
                "email",
                "userEmail",
                "emailAddress",
            ],
        )

        email = clean_string(
            email
        )

        if not email:

            continue

        user_type = get_first_value(
            user,
            [
                "type",
                "userType",
                "role",
            ],
        )

        user_type = clean_string(
            user_type
        )

        combined_records.append(
            {
                "storeId":
                    store_id,

                "storeName":
                    store[
                        "storeName"
                    ],

                "email":
                    email,

                "type":
                    user_type,
            }
        )


    print(
        f"Store-user relationships found: "
        f"{len(combined_records)}"
    )


    # --------------------------------------------------------
    # Save JSON
    # --------------------------------------------------------

    with open(
        COMBINED_JSON_FILE,
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            combined_records,
            file,
            indent=2,
            ensure_ascii=False,
            default=str,
        )


    # --------------------------------------------------------
    # Save CSV
    # --------------------------------------------------------

    combined_dataframe = pd.DataFrame(
        combined_records
    )

    combined_dataframe.to_csv(
        COMBINED_CSV_FILE,
        index=False,
        encoding="utf-8",
    )


    print(
        f"Saved combined JSON: "
        f"{COMBINED_JSON_FILE}"
    )

    print(
        f"Saved combined CSV: "
        f"{COMBINED_CSV_FILE}"
    )


    return combined_records


# ============================================================
# 23. BUILD EMAIL LOOKUP
# ============================================================

def build_email_lookup(
    combined_records: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:

    email_lookup = {}

    # --------------------------------------------------------
    # Prefer propertyOwner when multiple users exist.
    # --------------------------------------------------------

    priority = {
        "propertyowner": 1,
        "property_owner": 1,
        "owner": 1,
        "biller": 2,
    }

    for record in combined_records:

        store_id = normalize_store_id(
            record.get("storeId")
        )

        email = clean_string(
            record.get("email")
        )

        user_type = clean_string(
            record.get("type")
        )

        if not store_id or not email:

            continue

        key = store_id

        current = email_lookup.get(
            key
        )

        if current is None:

            email_lookup[key] = {
                "storeId":
                    store_id,

                "storeName":
                    clean_string(
                        record.get(
                            "storeName"
                        )
                    ),

                "email":
                    email,

                "type":
                    user_type,
            }

            continue

        current_priority = priority.get(
            current[
                "type"
            ].lower(),
            99,
        )

        new_priority = priority.get(
            user_type.lower(),
            99,
        )

        if (
            new_priority
            < current_priority
        ):

            email_lookup[key] = {
                "storeId":
                    store_id,

                "storeName":
                    clean_string(
                        record.get(
                            "storeName"
                        )
                    ),

                "email":
                    email,

                "type":
                    user_type,
            }

    return email_lookup


# ============================================================
# 24. UPDATE MISSING EMAILS
#
# IMPORTANT:
#   Only rows whose email is EMPTY are updated.
#
# Existing emails are preserved.
# ============================================================

def update_missing_emails(
    partner_csv_path: str,
    email_lookup: Dict[
        str,
        Dict[str, Any]
    ],
) -> Dict[str, Any]:

    print()
    print(
        f"Processing email update: "
        f"{partner_csv_path}"
    )

    if not os.path.exists(
        partner_csv_path
    ):

        print(
            f"WARNING: File does not exist: "
            f"{partner_csv_path}"
        )

        return {
            "skipped": True,
            "reason": "file_not_found",
        }


    dataframe = pd.read_csv(
        partner_csv_path,
        dtype=str,
        keep_default_na=False,
    )


    if PARTNER_ID_COLUMN not in dataframe.columns:

        raise KeyError(
            f"'{PARTNER_ID_COLUMN}' "
            "column not found in "
            f"{partner_csv_path}"
        )


    if "email" not in dataframe.columns:

        dataframe["email"] = ""


    name_column = (
        find_partner_name_column(
            dataframe
        )
    )


    rows_checked = 0

    email_updates = 0

    stores_not_found = 0

    matched_but_no_email = 0

    missing_email_rows = []

    updated_rows = []


    # ========================================================
    # LOOP PARTNER ROWS
    # ========================================================

    for index in dataframe.index:

        current_email = clean_string(
            dataframe.at[
                index,
                "email",
            ]
        )

        # ----------------------------------------------------
        # Existing email:
        # preserve it.
        # ----------------------------------------------------

        if current_email:

            continue


        rows_checked += 1


        store_id = normalize_store_id(
            dataframe.at[
                index,
                PARTNER_ID_COLUMN,
            ]
        )

        store_name = clean_string(
            dataframe.at[
                index,
                name_column,
            ]
        )


        if not store_id:

            stores_not_found += 1

            missing_email_rows.append(
                {
                    "store":
                        store_id,

                    "storeName":
                        store_name,

                    "reason":
                        "missing_store_id",
                }
            )

            continue


        user_record = email_lookup.get(
            store_id
        )


        if user_record is None:

            stores_not_found += 1

            missing_email_rows.append(
                {
                    "store":
                        store_id,

                    "storeName":
                        store_name,

                    "reason":
                        "store_not_found_in_users",
                }
            )

            continue


        email = clean_string(
            user_record.get(
                "email"
            )
        )


        if not email:

            matched_but_no_email += 1

            missing_email_rows.append(
                {
                    "store":
                        store_id,

                    "storeName":
                        store_name,

                    "reason":
                        "matched_but_no_email",
                }
            )

            continue


        # ----------------------------------------------------
        # Update email.
        # ----------------------------------------------------

        dataframe.at[
            index,
            "email",
        ] = email


        email_updates += 1


        updated_rows.append(
            {
                "store":
                    store_id,

                "storeName":
                    store_name,

                "email":
                    email,

                "type":
                    clean_string(
                        user_record.get(
                            "type"
                        )
                    ),
            }
        )


        print(
            "EMAIL UPDATE | "
            f"Store={store_name} | "
            f"ID={store_id} | "
            f"Email={email} | "
            f"Type={user_record.get('type', '')}"
        )


    # ========================================================
    # BACKUP
    # ========================================================

    backup_path = None

    if email_updates > 0:

        backup_path = backup_file(
            partner_csv_path
        )

        atomic_write_csv(
            dataframe,
            partner_csv_path,
        )


    # ========================================================
    # SAVE MISSING EMAIL REPORT
    # ========================================================

    missing_email_dataframe = pd.DataFrame(
        missing_email_rows
    )

    missing_email_dataframe.to_csv(
        MISSING_STORES_CSV_FILE,
        index=False,
        encoding="utf-8",
    )


    # ========================================================
    # SUMMARY
    # ========================================================

    summary = {
        "rows_checked_for_missing_email":
            rows_checked,

        "email_updates":
            email_updates,

        "stores_not_found":
            stores_not_found,

        "matched_but_no_email":
            matched_but_no_email,

        "total_rows":
            len(dataframe),

        "backup":
            backup_path,
    }


    print()
    print(
        "Email update summary:"
    )

    print(
        summary
    )


    return summary


# ============================================================
# 25. MAIN PIPELINE
# ============================================================

print()
print("=" * 100)
print("STARTING STORE MASTER PIPELINE")
print("=" * 100)


# ============================================================
# STEP 2 — LOGIN
# ============================================================

session = create_session()

token = login(
    session
)


# ============================================================
# STEP 2 — FETCH STORES
# ============================================================

from_date, to_date = (
    get_api_date_range(
        LOOKBACK_DAYS
    )
)

print(
    f"Store API date range: "
    f"{from_date} -> {to_date}"
)


api_stores = fetch_all_api_stores(
    session=session,
    token=token,
    from_date=from_date,
    to_date=to_date,
)


if not api_stores:

    raise RuntimeError(
        "Store API returned no stores. "
        "partner.csv will NOT be modified."
    )


save_api_stores(
    api_stores
)


# ============================================================
# STEP 3 — MATCH RUNNING STORES
# ============================================================

matched_stores, unmatched_names = (
    match_running_stores(
        api_stores=api_stores,
        billing_store_names=billing_store_names,
    )
)


save_running_stores(
    matched_stores
)


save_unmatched_billing_stores(
    unmatched_names
)


if not matched_stores:

    raise RuntimeError(
        "No running stores matched with Store API. "
        "partner.csv will NOT be modified."
    )


# ============================================================
# STEP 4 — UPDATE PARTNER CSV FILES
# ============================================================

print()
print("=" * 100)
print("STEP 4 — UPDATE PARTNER CSV FILES")
print("=" * 100)


partner_update_summaries = {}


for partner_file in (
    PARTNER_CSV,
    PARTNER_MONTH_CSV,
):

    if not os.path.exists(
        partner_file
    ):

        print(
            f"WARNING: File does not exist: "
            f"{partner_file}"
        )

        continue


    summary = update_partner_csv(
        matched_stores=matched_stores,
        partner_csv_path=partner_file,
        dry_run=False,
    )


    partner_update_summaries[
        partner_file
    ] = summary


# ============================================================
# STEP 5 — FETCH USERS
# ============================================================

# Use a fresh authenticated session,
# exactly as the working email process does.

email_session = create_session()

email_token = login(
    email_session
)


users = fetch_all_users(
    session=email_session,
    token=email_token,
)


# ============================================================
# STEP 6 — MATCH STORE USERS
# ============================================================

combined_records = (
    combine_store_users(
        matched_stores=matched_stores,
        users=users,
    )
)


# ============================================================
# STEP 7 — BUILD EMAIL LOOKUP
# ============================================================

email_lookup = build_email_lookup(
    combined_records
)


print(
    f"Email lookup contains "
    f"{len(email_lookup)} stores."
)


# ============================================================
# STEP 8 — UPDATE MISSING EMAILS
# ============================================================

print()
print("=" * 100)
print("STEP 7 — UPDATE MISSING EMAILS")
print("=" * 100)


email_update_summaries = {}


for partner_file in (
    PARTNER_CSV,
    PARTNER_MONTH_CSV,
):

    if not os.path.exists(
        partner_file
    ):

        print(
            f"WARNING: File does not exist: "
            f"{partner_file}"
        )

        continue


    summary = update_missing_emails(
        partner_csv_path=partner_file,
        email_lookup=email_lookup,
    )


    email_update_summaries[
        partner_file
    ] = summary


# ============================================================
# FINAL VALIDATION OF partner.csv
# ============================================================

print()
print("=" * 100)
print("FINAL PARTNER.CSV VALIDATION")
print("=" * 100)


final_partner = pd.read_csv(
    PARTNER_CSV,
    dtype=str,
    keep_default_na=False,
)


final_partner[
    "_id"
] = final_partner[
    PARTNER_ID_COLUMN
].apply(
    normalize_store_id
)


final_name_column = (
    find_partner_name_column(
        final_partner
    )
)


final_partner[
    "_name"
] = final_partner[
    final_name_column
].apply(
    normalize_store_name
)


duplicate_identity = (
    final_partner[
        (
            final_partner[
                "_id"
            ] != ""
        )
        &
        (
            final_partner[
                "_name"
            ] != ""
        )
    ]

    .groupby(
        [
            "_id",
            "_name",
        ]
    )

    .size()

    .reset_index(
        name="count"
    )

    .query(
        "count > 1"
    )
)


if len(
    duplicate_identity
) > 0:

    print(
        "❌ DUPLICATES STILL EXIST:"
    )

    display(
        duplicate_identity
    )

    raise RuntimeError(
        "FINAL VALIDATION FAILED: "
        "partner.csv contains duplicate "
        "store_id + normalized_store_name identities."
    )


print(
    "✅ No duplicate "
    "store_id + normalized_store_name "
    "identities found."
)


# ============================================================
# CHECK ACTIVE RUNNING STORES
# ============================================================

final_partner_ids = set(
    final_partner[
        "_id"
    ].tolist()
)


missing_active_stores = []

for store in matched_stores:

    store_id = normalize_store_id(
        store.get("storeId")
    )

    if (
        store_id
        and store_id
        not in final_partner_ids
    ):

        missing_active_stores.append(
            {
                "storeId":
                    store_id,

                "storeName":
                    clean_string(
                        store.get(
                            "storeName"
                        )
                    ),
            }
        )


if missing_active_stores:

    print(
        "❌ Some running stores are "
        "still missing from partner.csv:"
    )

    display(
        pd.DataFrame(
            missing_active_stores
        )
    )

    raise RuntimeError(
        "FINAL VALIDATION FAILED: "
        "One or more running stores are "
        "missing from partner.csv."
    )


print(
    "✅ All running stores are present "
    "in partner.csv."
)


# ============================================================
# CLEAN VALIDATION COLUMNS
# ============================================================

final_partner = final_partner.drop(
    columns=[
        "_id",
        "_name",
    ],
    errors="ignore",
)


# ============================================================
# FINAL SUMMARY
# ============================================================

print()
print("=" * 100)
print("FINAL PIPELINE SUMMARY")
print("=" * 100)

print(
    f"Running stores from Silver : "
    f"{len(billing_store_names)}"
)

print(
    f"API stores fetched         : "
    f"{len(api_stores)}"
)

print(
    f"Running stores matched     : "
    f"{len(matched_stores)}"
)

print(
    f"Unmatched running stores   : "
    f"{len(unmatched_names)}"
)

print()

print(
    "Partner store update:"
)

for file_path, summary in (
    partner_update_summaries.items()
):

    print(
        f"  {file_path}:"
    )

    print(
        f"    {summary}"
    )


print()

print(
    "Partner email update:"
)

for file_path, summary in (
    email_update_summaries.items()
):

    print(
        f"  {file_path}:"
    )

    print(
        f"    {summary}"
    )


print()
print("=" * 100)
print(
    "✅ PIPELINE COMPLETED SUCCESSFULLY"
)
print("=" * 100)

# COMMAND ----------

