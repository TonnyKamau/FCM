
import os
import time
import requests
import re
from datetime import datetime
from urllib.parse import quote_plus
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
import pytz
import logging
from dotenv import load_dotenv

from firebase_utils import get_db

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
MONGODB_URI = os.getenv("MONGODB_URI")
PHP_API_URL = os.getenv("PHP_API")

if not MONGODB_URI:
    print("Error: MONGODB_URI environment variable not set")
    exit(1)

if not PHP_API_URL:
    print("Error: PHP_API environment variable not set")
    exit(1)

def get_nairobi_time(date_obj):
    """Convert a UTC or naive date object to Nairobi time."""
    nairobi_tz = pytz.timezone('Africa/Nairobi')
    if date_obj.tzinfo is None:
        # Assume input is naive local time matching Nairobi if not specified, 
        # but logic in TS says we construct it then convert.
        # TS logic: new Date(y, m, d...) then toLocaleString("en-US", {timeZone: "Africa/Nairobi"})
        # We'll assume the parsed components represent local time in Nairobi directly.
        return nairobi_tz.localize(date_obj)
    return date_obj.astimezone(nairobi_tz)

def parse_formatted_date(date_str):
    """Parse FORMATTED_DATE string (e.g. '2024-05-23 14:30:00') to datetime object."""
    try:
        # Split by -, space, or :
        parts = re.split(r'[- :]', date_str)
        parts = [int(p) for p in parts if p]
        
        if len(parts) >= 6:
            # Create datetime object (assuming the string is already in Nairobi time or local)
            dt = datetime(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5])
            return get_nairobi_time(dt)
    except Exception as e:
        print(f"Warning: Invalid date format '{date_str}': {e}")
    
    return get_nairobi_time(datetime.now())

def fetch_transactions_with_retry(url, retries=3, timeout=10):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json, text/plain, */*'
    }
    for attempt in range(retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            if response.status_code >= 500 and attempt < retries:
                raise Exception(f"Server error {response.status_code}")
            
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if attempt == retries:
                raise Exception(f"Failed to fetch after {retries + 1} attempts: {e}")
            
            backoff = min(2 * (2 ** attempt), 10)
            print(f"Fetch attempt {attempt + 1} failed: {e}. Retrying in {backoff}s...")
            time.sleep(backoff)


def sync_transactions():
    print(f"[{datetime.now()}] Starting transaction sync...")
    
    try:
        # Connect to MongoDB
        client = MongoClient(MONGODB_URI)
        
        try:
            db = client.get_database() # Connects to default database in URI
        except Exception:
            print("No default database in URI, falling back to 'test' (Mongoose default)")
            db = client["test"]
        
        users_collection = db["users"]
        sessions_collection = db["sessions"]
        transactions_collection = db["transactions"]
        
        # Check for active sessions
        active_sessions_count = sessions_collection.count_documents({"expires": {"$gt": datetime.now()}})
        
        if active_sessions_count == 0:
            print("No active sessions found, continuing with sync (user-specific updates will be skipped)")
        
        # Fetch active sessions with user details
        active_sessions = []
        if active_sessions_count > 0:
            active_sessions = list(sessions_collection.aggregate([
                {"$match": {"expires": {"$gt": datetime.now()}}},
                {"$lookup": {
                    "from": "users",
                    "localField": "userId",
                    "foreignField": "_id",
                    "as": "user"
                }},
                {"$unwind": "$user"}
            ]))
        
        if active_sessions_count > 0 and not active_sessions:
            print("Warning: Active sessions found but user lookup failed")

        # FETCH RECENT CODES: Get last 200 transaction codes to avoid redundant DB checks
        print("Pre-fetching recent transaction codes...")
        recent_txs = list(transactions_collection.find({}, {"transactionCode": 1})
                          .sort("transactionDate", -1)
                          .limit(200))
        recent_codes_set = {tx.get("transactionCode") for tx in recent_txs if tx.get("transactionCode")}
        print(f"Loaded {len(recent_codes_set)} recent codes into cache.")

        # Fetch transactions from PHP API
        php_transactions = fetch_transactions_with_retry(PHP_API_URL, timeout=12)
        print(f"Fetched {len(php_transactions)} transactions from PHP API")
        
        user_balances = {}  # userId -> {balance, date}
        consecutive_existing = 0
        STOP_THRESHOLD = 50 # Stop if we see 50 already-synced transactions in a row (if sorted)
        
        for t in php_transactions:
            try:
                # Parse Data
                transaction_code = t.get("TRANSACTION CODE", "").strip().upper()
                amount = float(t.get("AMOUNT", 0))
                account_balance = float(t.get("ACCOUNT BALANCE", 0))
                customer = t.get("CUSTOMER")
                raw_date = t.get("FORMATTED_DATE", "")
                tx_type = t.get("TYPE", "")
                
                date_obj = parse_formatted_date(raw_date)
                
                # Check duplication locally first (High performance)
                if transaction_code in recent_codes_set:
                    consecutive_existing += 1
                    if consecutive_existing >= STOP_THRESHOLD:
                        print(f"Hit {STOP_THRESHOLD} consecutive existing transactions. Early exit.")
                        break
                    continue
                
                # Double check with DB just in case it's older than the pre-fetched list
                existing_tx = transactions_collection.find_one({"transactionCode": transaction_code})
                
                # Reset consecutive counter if it's new
                if not existing_tx:
                    consecutive_existing = 0
                else:
                    recent_codes_set.add(transaction_code) # Add to set for next pass
                    print(f"Transaction {transaction_code} already exists in DB, skipping")
                    continue
                
                # Create New Transaction
                # Only if we have at least one active user (as per Node.js logic)
                if active_sessions:
                    # Update User Balances Logic (Only for NEW transactions to avoid redundant processing)
                    for session in active_sessions:
                        user_id = str(session["userId"])
                        if user_id not in user_balances or date_obj > user_balances[user_id]["date"]:
                            user_balances[user_id] = {"balance": account_balance, "date": date_obj}

                    primary_user_id = active_sessions[0]["userId"]
                    
                    new_tx = {
                        "user": primary_user_id,
                        "customer": customer,
                        "type": "debit" if tx_type.lower() == "withdrawal" else "credit",
                        "transactionCode": transaction_code,
                        "amount": abs(amount),
                        "status": "completed",
                        "transactionDate": date_obj,
                        "accountBalance": account_balance,
                        "createdAt": datetime.now()
                    }
                    
                    try:
                        transactions_collection.insert_one(new_tx)
                        print(f"Saved transaction {transaction_code} for user {primary_user_id}")
                    except DuplicateKeyError:
                        print(f"Transaction {transaction_code} already exists (unique constraint), skipping")
                else:
                    print(f"No active session to assign transaction {transaction_code} to. Skipping save.")
                    
            except Exception as e:
                print(f"Error processing transaction {t.get('TRANSACTION CODE')}: {e}")

        # Update Users Balances
        for user_id, data in user_balances.items():
            try:
                from bson.objectid import ObjectId
                users_collection.update_one(
                    {"_id": ObjectId(user_id)},
                    {"$set": {"balance": data["balance"]}}
                )
                print(f"Updated balance for user {user_id} to {data['balance']}")
            except Exception as e:
                print(f"Failed to update balance for user {user_id}: {e}")

        client.close()
        

        print("Transaction sync completed successfully")
        
    except Exception as e:
        print(f"Error in transaction sync: {e}")

if __name__ == "__main__":
    sync_transactions()
