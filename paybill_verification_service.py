"""
Python port of PHP PaybillVerificationService.

Handles PAYBILL transaction verification and addition.
Uses Firestore with same paths as other services:
  - TRANSACTIONS/{userId}/{monthCollection}/{transactionId}
  - SAVINGS/{userId}/accounts/{accountType}
"""

import logging
import re
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

from config import PAYMENT_API_URL
from firebase_utils import get_db

logger = logging.getLogger(__name__)


class PaybillVerificationService:
    _processed_transactions: List[str] = []

    def __init__(self) -> None:
        self.db = get_db()
        self.payment_api_url = PAYMENT_API_URL
        logger.info("PaybillVerificationService initialized")

    # ───────────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ───────────────────────────────────────────────────────────────────────────
    def _fetch_payments_from_api(self) -> Optional[List[Dict[str, Any]]]:
        try:
            headers = {"User-Agent": "curl/7.81.0", "Accept": "*/*"}
            resp = requests.get(self.payment_api_url, timeout=30, headers=headers)
        except Exception as exc:
            logger.error("Payment API request failed: %s", exc)
            return None

        if resp.status_code != 200:
            logger.error("Payment API returned HTTP %s: %s", resp.status_code, resp.text[:500])
            return None

        try:
            data = resp.json()
        except ValueError as exc:
            logger.error("Failed to decode payment API JSON: %s", exc)
            return None

        if not isinstance(data, list):
            logger.error("Unexpected payment API payload type: %s", type(data))
            return None

        return data

    @staticmethod
    def _get_current_month() -> str:
        """Returns current month like 'Nov'."""
        return datetime.now().strftime("%b")

    @staticmethod
    def _generate_uuid() -> str:
        """Generate UUID (matches Android UUID.randomUUID().toString())."""
        return str(uuid.uuid4())

    def _transaction_exists(self, user_id: str, transaction_code: str) -> bool:
        """Check if transaction already exists in any month subcollection."""
        try:
            user_ref = self.db.collection("TRANSACTIONS").document(user_id)
            for month_collection in user_ref.collections():
                for doc in month_collection.stream():
                    data = doc.to_dict() or {}
                    if data.get("transactionCode") == transaction_code:
                        return True
            return False
        except Exception as exc:
            logger.exception("Transaction exists check error: %s", exc)
            return False

    def _add_paybill_transaction(self, user_id: str, payment: Dict[str, Any]) -> Dict[str, Any]:
        """Add PAYBILL transaction to Firestore and update balance."""
        try:
            transaction_id = self._generate_uuid()
            amount = float(payment.get("AMOUNT", 0))
            transaction_code = payment.get("TRANSACTION CODE", "")
            customer = payment.get("CUSTOMER", "")
            account_reference = payment.get("ACCOUNT REFERENCE", "")
            formatted_date = payment.get("FORMATTED_DATE", "")

            # Parse timestamp
            # Parse timestamp from verified transaction
            timestamp = int(time.time() * 1000)  # Default to current time
            if formatted_date:
                try:
                    # Robust parsing similar to sync_transactions.py
                    # Split by -, space, or :
                    parts = re.split(r'[- :]', formatted_date)
                    parts = [int(p) for p in parts if p]
                    
                    if len(parts) >= 6:
                        dt = datetime(parts[0], parts[1], parts[2], parts[3], parts[4], parts[5])
                        timestamp = int(dt.timestamp() * 1000)
                except Exception as e:
                    logger.warning(f"Failed to parse date '{formatted_date}': {e}")

            current_month = self._get_current_month()

            # Build transaction data (matches Android TransactionModel)
            transaction_data = {
                "id": transaction_id,
                "amount": amount,
                "type": "NORMAL",
                "paymentMethod": "PAYBILL",
                "status": "DONE",
                "transactionCode": transaction_code,
                "accountType": "NORMAL",
                "mobileNumber": customer,
                "accountReference": account_reference,
                "timestamp": timestamp,
            }

            # Save to TRANSACTIONS/{userId}/{month}/{transactionId}
            self.db.collection("TRANSACTIONS").document(user_id).collection(
                current_month
            ).document(transaction_id).set(transaction_data)

            logger.info("Added PAYBILL transaction: %s for user: %s", transaction_code, user_id)

            # Update user balance
            balance_result = self._update_user_balance(user_id, amount)

            return {
                "success": True,
                "transactionCode": transaction_code,
                "amount": amount,
                "newBalance": balance_result.get("newBalance", 0),
            }

        except Exception as exc:
            logger.exception("Add PAYBILL transaction error: %s", exc)
            return {"success": False, "error": str(exc)}

    def _update_user_balance(self, user_id: str, amount: float) -> Dict[str, Any]:
        """Update user balance in SAVINGS/{userId}/accounts/NORMAL.

        Uses fs.Increment (atomic server-side op) instead of a read-then-SET so
        that two concurrent Paybill verifications for the same user cannot race:
          • Old approach: read balance → add locally → SET new_balance
            Risk: if two workers both read the same old_balance they both write
            the same new_balance, effectively losing one credit entirely.
          • New approach: fs.Increment lets Firestore's server add atomically,
            so every call contributes its amount regardless of concurrency.
        """
        try:
            from google.cloud import firestore as fs  # local import avoids circular

            saving_ref = (
                self.db.collection("SAVINGS")
                .document(user_id)
                .collection("accounts")
                .document("NORMAL")
            )

            # Read old balance for logging only — not used in the write
            snap = saving_ref.get()
            old_balance = 0.0
            if snap.exists:
                data = snap.to_dict() or {}
                old_balance = float(data.get("amount", 0))

            # Atomic server-side increment — safe against concurrent updates
            saving_ref.set(
                {
                    "id": "NORMAL",
                    "amount": fs.Increment(float(amount)),
                    "accountType": "NORMAL",
                    "userId": user_id,
                    "lastUpdated": int(time.time() * 1000),
                },
                merge=True,
            )

            new_balance = old_balance + float(amount)  # approximate for display
            logger.info(
                "Updated balance with PAYBILL payment: %s (Old: %s, New: ~%s)",
                amount, old_balance, new_balance,
            )

            return {"success": True, "oldBalance": old_balance, "newBalance": new_balance}

        except Exception as exc:
            logger.exception("Balance update error: %s", exc)
            return {"success": False, "error": str(exc), "newBalance": 0}

    def _check_and_add_paybill_transaction(
        self, user_id: str, payment: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Check if transaction exists and add if not."""
        transaction_code = payment.get("TRANSACTION CODE", "")

        if not transaction_code:
            return {"success": False, "error": "Missing transaction code"}

        # Skip if already processed in this run
        if transaction_code in self._processed_transactions:
            return {"success": False, "error": "Already processed in this run"}

        # Check if transaction exists in database
        if self._transaction_exists(user_id, transaction_code):
            self._processed_transactions.append(transaction_code)
            logger.info("PAYBILL transaction already exists: %s", transaction_code)
            return {"success": False, "error": "Transaction already exists"}

        # Add the PAYBILL transaction
        result = self._add_paybill_transaction(user_id, payment)

        if result.get("success"):
            self._processed_transactions.append(transaction_code)
            if len(self._processed_transactions) > 100:
                self._processed_transactions.pop(0)

        return result

    # ───────────────────────────────────────────────────────────────────────────
    # Public API
    # ───────────────────────────────────────────────────────────────────────────
    def process_paybill_for_user(
        self, user_id: str, user_account_number: str, payments: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Process PAYBILL transactions for a specific user.
        Pass pre-fetched payments for speed when processing multiple users.
        """
        try:
            if payments is None:
                payments = self._fetch_payments_from_api()

            if not payments:
                return {"success": False, "error": "Failed to fetch payment data from API"}

            added_count = 0
            results: List[Dict[str, Any]] = []

            for payment in payments:
                payment_method = payment.get("PAYMENTMETHOD", "")

                # Only process PAYBILL payments
                if payment_method != "PAYBILL":
                    continue

                account_reference = payment.get("ACCOUNT REFERENCE", "") or ""

                # Check if account reference matches user's account number
                if (
                    account_reference.lower() != user_account_number.lower()
                    and user_account_number.lower() not in account_reference.lower()
                ):
                    continue

                # Check and add the PAYBILL transaction
                result = self._check_and_add_paybill_transaction(user_id, payment)

                if result.get("success"):
                    added_count += 1
                    results.append(result)

            return {"success": True, "addedCount": added_count, "results": results}

        except Exception as exc:
            logger.exception("Process PAYBILL error for user %s: %s", user_id, exc)
            return {"success": False, "error": str(exc)}

    @classmethod
    def cleanup_cache(cls) -> None:
        while len(cls._processed_transactions) > 50:
            cls._processed_transactions.pop(0)
        logger.info("Cache cleanup: processed=%d", len(cls._processed_transactions))

