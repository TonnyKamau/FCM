import os
import re
import time
from datetime import datetime, timezone

import pytz
import requests
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
PHP_API_URL = os.getenv("PHP_API")

if not SUPABASE_URL:
    print("Error: SUPABASE_URL environment variable not set")
    raise SystemExit(1)

if not SUPABASE_SERVICE_ROLE_KEY:
    print("Error: SUPABASE_SERVICE_ROLE_KEY environment variable not set")
    raise SystemExit(1)

if not PHP_API_URL:
    print("Error: PHP_API environment variable not set")
    raise SystemExit(1)

REST_BASE = f"{SUPABASE_URL.rstrip('/')}/rest/v1"
REST_HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json",
}


def supabase_get(table, params=None):
    response = requests.get(f"{REST_BASE}/{table}", headers=REST_HEADERS, params=params, timeout=20)
    response.raise_for_status()
    return response.json()


def supabase_post(table, payload):
    headers = {**REST_HEADERS, "Prefer": "return=representation"}
    response = requests.post(f"{REST_BASE}/{table}", headers=headers, json=payload, timeout=20)
    response.raise_for_status()
    return response.json()


def supabase_patch(table, filters, payload):
    headers = {**REST_HEADERS, "Prefer": "return=representation"}
    response = requests.patch(
        f"{REST_BASE}/{table}",
        headers=headers,
        params=filters,
        json=payload,
        timeout=20,
    )
    response.raise_for_status()
    return response.json()


def get_nairobi_time(date_obj):
    nairobi_tz = pytz.timezone("Africa/Nairobi")
    if date_obj.tzinfo is None:
        return nairobi_tz.localize(date_obj)
    return date_obj.astimezone(nairobi_tz)


def parse_formatted_date(date_str):
    try:
        parts = re.split(r"[- :]", date_str)
        parts = [int(p) for p in parts if p]
        if len(parts) >= 6:
            dt = datetime(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5])
            return get_nairobi_time(dt)
    except Exception as exc:
        print(f"Warning: Invalid date format '{date_str}': {exc}")
    return get_nairobi_time(datetime.now())


def fetch_transactions_with_retry(url, retries=3, timeout=10):
    header_profiles = [
        {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/plain, */*",
            "label": "browser",
        },
        {
            "User-Agent": "curl/7.81.0",
            "Accept": "*/*",
            "label": "curl",
        },
    ]
    for attempt in range(retries + 1):
        try:
            headers = header_profiles[min(attempt, len(header_profiles) - 1)]
            response = requests.get(url, headers=headers, timeout=timeout)
            if response.status_code >= 500 and attempt < retries:
                raise Exception(f"Server error {response.status_code}")
            if response.status_code >= 400:
                body_preview = response.text[:500].replace("\n", " ")
                raise Exception(
                    f"HTTP {response.status_code} from PHP API. "
                    f"Response preview: {body_preview}"
                )
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            if attempt == retries:
                raise Exception(f"Failed to fetch after {retries + 1} attempts: {exc}")
            backoff = min(2 * (2 ** attempt), 10)
            print(
                f"Fetch attempt {attempt + 1} ({headers['label']}) failed: "
                f"{exc}. Retrying in {backoff}s..."
            )
            time.sleep(backoff)


def sync_transactions():
    print(f"[{datetime.now()}] Starting transaction sync...")
    try:
        active_sessions = supabase_get(
            "sessions",
            params={
                "select": "user_id,expires",
                "expires": f"gt.{datetime.now(timezone.utc).isoformat()}",
            },
        )

        user_rows = supabase_get(
            "users",
            params={"select": "id,email,role,created_at", "order": "created_at.asc"},
        )

        sync_user_ids = [row["user_id"] for row in active_sessions if row.get("user_id")]
        if not sync_user_ids and user_rows:
            sync_user_ids = [row["id"] for row in user_rows]
            print(
                "No active sessions found, falling back to existing Supabase users "
                f"for transaction ownership and balance updates ({len(sync_user_ids)} user(s))"
            )
        elif not sync_user_ids:
            print("No active sessions or users found, transactions will be saved without user ownership")

        existing_transactions = supabase_get(
            "transactions",
            params={"select": "id,transaction_code,transaction_date"},
        )
        existing_codes = {row["transaction_code"] for row in existing_transactions}

        latest_synced_date = None
        if existing_transactions:
            latest_synced_date = max(
                datetime.fromisoformat(row["transaction_date"].replace("Z", "+00:00"))
                for row in existing_transactions
                if row.get("transaction_date")
            )
            print(f"Latest synced date: {latest_synced_date}")
        else:
            print("No transactions in Supabase yet, syncing all available rows")

        php_transactions = fetch_transactions_with_retry(PHP_API_URL, timeout=12)
        print(f"Fetched {len(php_transactions)} transactions from PHP API")

        user_balances = {}
        latest_feed_balance = None
        latest_feed_date = None
        new_count = 0
        skipped_old = 0
        skipped_dup = 0

        for transaction in php_transactions:
            try:
                transaction_code = transaction.get("TRANSACTION CODE", "").strip().upper()
                amount = float(transaction.get("AMOUNT", 0))
                account_balance = float(transaction.get("ACCOUNT BALANCE", 0))
                customer = transaction.get("CUSTOMER")
                raw_date = transaction.get("FORMATTED_DATE", "")
                tx_type = transaction.get("TYPE", "")

                date_obj = parse_formatted_date(raw_date).astimezone(timezone.utc)

                if latest_feed_date is None or date_obj > latest_feed_date:
                    latest_feed_date = date_obj
                    latest_feed_balance = account_balance

                if latest_synced_date is not None and date_obj <= latest_synced_date:
                    skipped_old += 1
                    continue

                if transaction_code in existing_codes:
                    skipped_dup += 1
                    continue

                if sync_user_ids:
                    for user_id in sync_user_ids:
                        current = user_balances.get(user_id)
                        if current is None or date_obj > current["date"]:
                            user_balances[user_id] = {"balance": account_balance, "date": date_obj}

                    supabase_post(
                        "transactions",
                        {
                            "user_id": None,
                            "customer": customer,
                            "type": "debit" if tx_type.lower() == "withdrawal" else "credit",
                            "transaction_code": transaction_code,
                            "amount": abs(amount),
                            "status": "completed",
                            "transaction_date": date_obj.isoformat(),
                            "account_balance": account_balance,
                        },
                    )
                    existing_codes.add(transaction_code)
                    new_count += 1
                    print(f"Saved shared transaction {transaction_code}")
                else:
                    supabase_post(
                        "transactions",
                        {
                            "user_id": None,
                            "customer": customer,
                            "type": "debit" if tx_type.lower() == "withdrawal" else "credit",
                            "transaction_code": transaction_code,
                            "amount": abs(amount),
                            "status": "completed",
                            "transaction_date": date_obj.isoformat(),
                            "account_balance": account_balance,
                        },
                    )
                    existing_codes.add(transaction_code)
                    new_count += 1
                    print(f"Saved transaction {transaction_code} without a linked user")
            except Exception as exc:
                print(f"Error processing transaction {transaction.get('TRANSACTION CODE')}: {exc}")

        if latest_feed_balance is not None:
            target_user_ids = [row["id"] for row in user_rows if row.get("id")]
            for user_id in target_user_ids:
                current = user_balances.get(user_id)
                if current is None or latest_feed_date is not None and latest_feed_date > current["date"]:
                    user_balances[user_id] = {"balance": latest_feed_balance, "date": latest_feed_date}

        for user_id, data in user_balances.items():
            try:
                supabase_patch(
                    "users",
                    {"id": f"eq.{user_id}", "select": "id"},
                    {"balance": data["balance"]},
                )
                print(f"Updated balance for user {user_id} to {data['balance']}")
            except Exception as exc:
                print(f"Failed to update balance for user {user_id}: {exc}")

        print(
            f"Sync summary: {new_count} new, {skipped_old} old (before watermark), {skipped_dup} duplicates"
        )
        print("Transaction sync completed successfully")
    except Exception as exc:
        print(f"Error in transaction sync: {exc}")


if __name__ == "__main__":
    sync_transactions()
