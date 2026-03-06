import logging
import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from google.cloud.firestore_v1.base_query import FieldFilter

import requests
from google.cloud import firestore as fs

from config import PULL_API_URL
from firebase_utils import get_db

logger = logging.getLogger(__name__)


def _normalize_phone(phone: str) -> str:
    """Normalize phone number to 2547XXXXXXXX format."""
    if not phone:
        return ""
    # Remove any non-digit characters
    digits = "".join(filter(str.isdigit, str(phone)))
    if digits.startswith("0"):
        return "254" + digits[1:]
    if digits.startswith("7"):
        return "254" + digits
    if digits.startswith("+"):
        return digits[1:]
    return digits


def _parse_pull_time(time_str: str) -> float:
    """Parse Pull API TransTime (YYYYMMDDHHMMSS) to unix timestamp."""
    try:
        # Example: 20260218000258
        dt = datetime.strptime(time_str, "%Y%m%d%H%M%S")
        return dt.timestamp()
    except (ValueError, TypeError):
        return 0.0


class PullVerificationService:
    """
    Service to poll a pull API and verify pending "Direct mobile" transactions.
    Matches based on mobile number and amount.
    """

    def __init__(self) -> None:
        self.db = get_db()
        self.pull_api_url = PULL_API_URL
        logger.info("PullVerificationService initialized with URL: %s", self.pull_api_url)

    def _fetch_pull_data(self) -> Optional[List[Dict[str, Any]]]:
        try:
            # Mimic a generic curl request
            headers = {
                "User-Agent": "curl/7.81.0",
                "Accept": "*/*",
            }
            resp = requests.get(self.pull_api_url, timeout=30, headers=headers)
        except Exception as exc:
            logger.error("Pull API request failed: %s", exc)
            return None

        if resp.status_code != 200:
            logger.error(
                "Pull API returned HTTP %s: %s",
                resp.status_code,
                resp.text[:500],
            )
            return None

        try:
            text = resp.text.strip()
            # Handle concatenated JSON objects like "{...} {...}"
            # We can use JSONDecoder.raw_decode in a loop
            decoder = json.JSONDecoder()
            pos = 0
            data = []
            while pos < len(text):
                # Skip any leading whitespace/newlines between objects
                while pos < len(text) and text[pos].isspace():
                    pos += 1
                if pos >= len(text):
                    break
                try:
                    obj, pos = decoder.raw_decode(text, pos)
                    data.append(obj)
                except json.JSONDecodeError as exc:
                    logger.error("Error decoding JSON object at pos %d: %s", pos, exc)
                    # Try to skip characters until we find the next '{'
                    next_brace = text.find('{', pos + 1)
                    if next_brace == -1:
                        break
                    pos = next_brace

        except Exception as exc:
            logger.error("Failed to process Pull API response: %s", exc)
            return None

        if not data:
            logger.warning("No JSON objects found in Pull API response")
            return []

        return data

    def _update_user_balance(
        self,
        user_id: str,
        account_type: str,
        amount: float,
    ) -> Dict[str, Any]:
        """
        Updates the user's balance in SAVINGS/{userId}/accounts/{accountType}.
        Mirroring PaymentVerificationService._update_user_balance.
        """
        try:
            saving_ref = (
                self.db.collection("SAVINGS")
                .document(user_id)
                .collection("accounts")
                .document(account_type)
            )

            snap = saving_ref.get()
            old_balance = 0.0
            if snap.exists:
                data = snap.to_dict() or {}
                old_balance = float(data.get("amount", 0.0))

            # Atomic server-side increment — safe against concurrent updates
            saving_ref.set(
                {
                    "id": account_type,
                    "amount": fs.Increment(float(amount)),
                    "accountType": account_type,
                    "userId": user_id,
                    "lastUpdated": int(time.time() * 1000),
                },
                merge=True,
            )

            new_balance = old_balance + float(amount)  # approximate for display
            logger.info(
                "Updated balance for user=%s accountType=%s: %s -> ~%s (atomic increment)",
                user_id,
                account_type,
                old_balance,
                new_balance,
            )

            return {
                "success": True,
                "oldBalance": old_balance,
                "newBalance": new_balance,
            }
        except Exception as exc:
            logger.exception("Balance update error for user %s: %s", user_id, exc)
            return {"success": False, "error": str(exc)}

    def process_all_pending(self) -> Dict[str, Any]:
        """
        Fetches pull data and checks all users for pending "Direct mobile" transactions.
        """
        try:
            logger.info("Fetching data from Pull API: %s", self.pull_api_url)
            pull_data = self._fetch_pull_data()
            if pull_data is None:
                return {"success": False, "error": "Failed to fetch pull data"}

            if not pull_data:
                logger.info("No records found in Pull API")
                return {"success": True, "verifiedCount": 0, "message": "No data in Pull API"}

            logger.info("Querying TRANSACTIONS collection...")
            transactions_collection = self.db.collection("TRANSACTIONS")
            user_docs = list(transactions_collection.stream())
            logger.info("Found %d users with potential transactions.", len(user_docs))
            
            total_verified = 0
            results = []

            for user_doc in user_docs:
                user_id = user_doc.id
                user_ref = transactions_collection.document(user_id)
                
                # Check each month collection under the user
                for month_collection in user_ref.collections():
                    for doc in month_collection.stream():
                        data = doc.to_dict() or {}
                        status = data.get("status", "")
                        payment_method = data.get("paymentMethod", "")
                        transaction_code = data.get("transactionCode")
                        
                        # Only process PENDING or PROCESSING transactions that don't have a code yet
                        if status not in {"PENDING", "PROCESSING"} or transaction_code:
                            continue

                        # Normalize payment method check as it might be "Direct Mobile - CheckoutRequestID:..."
                        is_direct_mobile = "Direct mobile" in payment_method or "Direct Mobile" in payment_method
                        
                        if not is_direct_mobile:
                            continue

                        # Correct Firestore keys from screenshot: mobileNumber, amount, timestamp
                        mobile_number = data.get("mobileNumber") or data.get("phoneNumber") or data.get("mobilenumber")
                        
                        try:
                            amount = float(data.get("amount", 0))
                        except (ValueError, TypeError):
                            amount = 0.0
                            
                        # Firestore timestamp is likely in milliseconds (e.g., 1771333111063)
                        firestore_ts_raw = data.get("timestamp", 0)
                        try:
                            firestore_ts = float(firestore_ts_raw) / 1000.0 if firestore_ts_raw > 1e11 else float(firestore_ts_raw)
                        except (ValueError, TypeError):
                            firestore_ts = 0.0

                        if not mobile_number or amount <= 0:
                            logger.warning("Transaction %s has invalid data: mobile=%s, amount=%s", doc.id, mobile_number, amount)
                            continue

                        norm_mobile = _normalize_phone(mobile_number)
                        logger.info("Processing transaction %s: Phone=%s (%s), Amount=%s, RawTS=%s, ParsedTS=%s", 
                                    doc.id, norm_mobile, mobile_number, amount, firestore_ts_raw, datetime.fromtimestamp(firestore_ts))

                        # Try to find a match in pull_data
                        match_found = False
                        if not pull_data:
                            logger.warning("Pull Data is empty!")
                        
                        for record in pull_data:
                            record_msisdn = _normalize_phone(record.get("MSISDN", ""))
                            try:
                                record_amount = float(record.get("TransAmount", 0))
                            except (ValueError, TypeError):
                                record_amount = 0.0
                                
                            record_code = record.get("TransID", "")
                            record_time_str = record.get("TransTime", "")
                            record_ts = _parse_pull_time(record_time_str)

                            # Time proximity: within 48 hours (172800 seconds)
                            time_diff = abs(firestore_ts - record_ts)
                            is_time_close = time_diff < 172800

                            # Detailed match check
                            mobile_match = norm_mobile == record_msisdn
                            amount_match = abs(amount - record_amount) < 0.01
                            
                            logger.debug("  Comparing with Pull Record %s: Phone=%s, Amount=%s, Time=%s (Diff: %.1fh, Match: P=%s, A=%s, T=%s)",
                                         record_code, record_msisdn, record_amount, datetime.fromtimestamp(record_ts), 
                                         time_diff/3600, mobile_match, amount_match, is_time_close)

                            if mobile_match and amount_match:
                                if is_time_close:
                                    # --- Duplicate Check ---
                                    # Check if this TransID already exists in Firestore as a 'transactionCode' 
                                    # to prevent double updates if the same record appears in Pull API again.
                                    already_exists = False
                                    
                                    # Check if current doc already has it
                                    if data.get("transactionCode") == record_code:
                                        already_exists = True
                                    
                                    if not already_exists:
                                        # Check across ALL collections for this specific user
                                        try:
                                            user_ref = self.db.collection("TRANSACTIONS").document(user_id)
                                            for coll in user_ref.collections():
                                                # Query this collection for the transactionCode
                                                existing_docs = coll.where(filter=FieldFilter("transactionCode", "==", record_code)).limit(1).get()
                                                if len(existing_docs) > 0:
                                                    already_exists = True
                                                    break
                                        except Exception as e:
                                            logger.warning("Error checking for duplicate TransID %s for user %s: %s", record_code, user_id, e)

                                    if already_exists:
                                        logger.info("  Record %s already processed in Firestore for user %s. Skipping.", record_code, user_id)
                                        continue

                                    logger.info("MATCH FOUND! Transaction %s matches Pull Record %s", doc.id, record_code)
                                    
                                    # Update transaction
                                    doc.reference.update({
                                        "status": "DONE",
                                        "transactionCode": record_code,
                                        "paymentMethod": "Direct Mobile - Verified",
                                        "verifiedAt": int(time.time() * 1000)
                                    })
                                    
                                    # Update balance
                                    account_type = data.get("accountType", "normal")
                                    balance_result = self._update_user_balance(user_id, account_type, amount)
                                    
                                    if balance_result.get("success"):
                                        logger.info("BALANCE UPDATED: User %s (%s) %s -> %s", 
                                                    user_id, account_type, balance_result['oldBalance'], balance_result['newBalance'])
                                    else:
                                        logger.error("BALANCE UPDATE FAILED for user %s: %s", user_id, balance_result.get("error"))
                                    
                                    total_verified += 1
                                    match_found = True
                                    results.append({
                                        "userId": user_id,
                                        "transactionId": doc.id,
                                        "transactionCode": record_code,
                                        "amount": amount,
                                        "oldBalance": balance_result.get("oldBalance"),
                                        "newBalance": balance_result.get("newBalance")
                                    })
                                    break
                                else:
                                    logger.debug("   Match found for phone/amount but time diff too large (%.1f hours)", 
                                                 time_diff / 3600.0)
                            
                        if not match_found:
                            logger.debug("No match found for transaction %s in this Pull API batch", doc.id)

            return {
                "success": True,
                "verifiedCount": total_verified,
                "results": results
            }

        except Exception as exc:
            logger.exception("Error in process_all_pending: %s", exc)
            return {"success": False, "error": str(exc)}

if __name__ == "__main__":
    # Setup logging to console
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger.info("=== Pull Verification Service (Standalone Run) ===")
    service = PullVerificationService()
    
    logger.info("Starting batch verification...")
    start_time = time.time()
    
    res = service.process_all_pending()
    
    end_time = time.time()
    duration = end_time - start_time
    
    if res.get("success"):
        count = res.get("verifiedCount", 0)
        logger.info("Verification completed successfully in %.2f seconds.", duration)
        logger.info("Total transactions verified: %d", count)
        if count > 0:
            for item in res.get("results", []):
                logger.info("  - [%s] Verified: %s, Amount: %s", 
                            item['userId'][:8], item['transactionCode'], item['amount'])
    else:
        logger.error("Verification failed: %s", res.get("error"))
    
    logger.info("=== Run Finished ===")
