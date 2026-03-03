"""
Python port of PHP WithdrawalVerificationService.

Uses the same subcollection approach as PaymentVerificationService:
  TRANSACTIONS/{userId}/{monthCollection}/{transactionId}
"""

import logging
import time
from typing import Any, Dict, List, Optional

import requests

from config import PAYMENT_API_URL
from firebase_utils import get_db

logger = logging.getLogger(__name__)


class WithdrawalVerificationService:
    _processed_transactions: List[str] = []
    _balance_updated_transactions: Dict[str, bool] = {}
    _failed_withdrawal_processed: Dict[str, bool] = {}

    def __init__(self) -> None:
        self.db = get_db()
        self.payment_api_url = PAYMENT_API_URL
        logger.info("WithdrawalVerificationService initialized")

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
    def _is_withdrawal_transaction(transaction: Dict[str, Any]) -> bool:
        tx_type = transaction.get("type", "")
        payment_method = transaction.get("paymentMethod", "")
        
        # B2C transactions are those that are NOT STK push ("Direct...")
        # and are explicitly marked as withdrawal types
        is_correct_type = tx_type == "WITHDRAW" or tx_type.startswith("GROUP_WITHDRAWAL_")
        is_b2c = not payment_method.startswith("Direct")
        
        return is_correct_type and is_b2c

    @staticmethod
    def _calculate_withdrawal_fee(amount: float) -> float:
        if amount <= 100:
            return 11
        if amount <= 500:
            return 22
        if amount <= 1000:
            return 29
        if amount <= 1500:
            return 29
        if amount <= 2500:
            return 52
        if amount <= 3500:
            return 69
        if amount <= 5000:
            return 87
        if amount <= 7500:
            return 115
        if amount <= 10000:
            return 115
        if amount <= 15000:
            return 167
        if amount <= 20000:
            return 185
        if amount <= 35000:
            return 197
        if amount <= 50000:
            return 278
        return 309

    # ───────────────────────────────────────────────────────────────────────────
    # Firestore updates (using subcollection approach like PaymentVerificationService)
    # ───────────────────────────────────────────────────────────────────────────
    def _update_withdrawal_transaction_as_done(
        self, doc_reference, transaction_id: str, transaction_code: str
    ) -> bool:
        """Update transaction document directly via its reference."""
        logger.info("Updating transaction to DONE: txId=%s code=%s", transaction_id, transaction_code)
        try:
            doc_reference.update({
                "status": "DONE",
                "transactionCode": transaction_code,
                "paymentMethod": "M-PESA B2C - Verified",
            })
            return True
        except Exception as exc:
            logger.exception("Error updating withdrawal transaction: %s", exc)
            return False

    def _update_related_fee_transactions(self, user_id: str, transaction_id: str) -> None:
        """Find and update fee transactions in subcollections."""
        try:
            user_ref = self.db.collection("TRANSACTIONS").document(user_id)
            for month_collection in user_ref.collections():
                for doc in month_collection.stream():
                    data = doc.to_dict() or {}
                    acc_ref = data.get("accountReference", "")
                    if acc_ref == f"FEE_FOR_{transaction_id}" or acc_ref.startswith(
                        f"WITHDRAWAL_FEE_FOR_{transaction_id}"
                    ):
                        doc.reference.update({"status": "DONE"})
                        logger.info("Updated related fee transaction: %s", doc.id)
        except Exception as exc:
            logger.exception("Error updating related fee transactions: %s", exc)

    def _update_withdrawal_transaction_as_failed(
        self, doc_reference, transaction_id: str, reason: str
    ) -> bool:
        logger.info("Updating transaction %s as FAILED: %s", transaction_id, reason)
        try:
            doc_reference.update({
                "status": "FAILED",
                "paymentMethod": f"M-PESA B2C - FAILED: {reason}",
            })
            return True
        except Exception as exc:
            logger.exception("Error updating failed withdrawal: %s", exc)
            return False

    def _update_related_fee_transactions_as_failed(self, user_id: str, transaction_id: str) -> None:
        try:
            user_ref = self.db.collection("TRANSACTIONS").document(user_id)
            for month_collection in user_ref.collections():
                for doc in month_collection.stream():
                    data = doc.to_dict() or {}
                    acc_ref = data.get("accountReference", "")
                    if acc_ref == f"FEE_FOR_{transaction_id}" or acc_ref.startswith(
                        f"WITHDRAWAL_FEE_FOR_{transaction_id}"
                    ):
                        doc.reference.update({"status": "FAILED"})
                        logger.info("Updated related fee transaction as FAILED: %s", doc.id)
        except Exception as exc:
            logger.exception("Error updating related fee transactions as failed: %s", exc)

    # def _reverse_withdrawal_deduction(
    #     self, user_id: str, account_type: str, withdrawal_amount: float, transaction_id: str
    # ) -> bool:
    #     fee = self._calculate_withdrawal_fee(withdrawal_amount)
    #     total = withdrawal_amount + fee
    #     logger.info(
    #         "Reversing withdrawal deduction: amount=%s fee=%s total=%s accountType=%s",
    #         withdrawal_amount, fee, total, account_type,
    #     )
    #     try:
    #         saving_ref = (
    #             self.db.collection("SAVINGS")
    #             .document(user_id)
    #             .collection("accounts")
    #             .document(account_type)
    #         )
    #         snap = saving_ref.get()
    #         if not snap.exists:
    #             logger.error("Savings account not found for reversal")
    #             return False
    #         data = snap.to_dict() or {}
    #         old_balance = float(data.get("amount", 0))
    #         new_balance = old_balance + total
    #         saving_ref.update({"amount": new_balance, "lastUpdated": int(time.time() * 1000)})
    #         logger.info("Balance restored: %s -> %s", old_balance, new_balance)
    #         return True
    #     except Exception as exc:
    #         logger.exception("Failed to restore balance: %s", exc)
    #         return False

    # ───────────────────────────────────────────────────────────────────────────
    # Public API
    # ───────────────────────────────────────────────────────────────────────────
    def verify_specific_withdrawal(
        self,
        user_id: str,
        account_reference: str,
        account_type: str,
        withdrawal_amount: float,
        is_group_withdrawal: bool = False,
        group_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Fast path verification triggered by Android client."""
        logger.info(
            "Quick withdrawal verification: userId=%s ref=%s type=%s amount=%s group=%s",
            user_id, account_reference, account_type, withdrawal_amount, is_group_withdrawal,
        )

        if account_reference in self._processed_transactions:
            return {"success": True, "verified": True, "message": "Already processed", "cached": True}

        payments = self._fetch_payments_from_api()
        if not payments:
            return {"success": False, "verified": False, "message": "Failed to fetch payment data from M-Pesa"}

        # Find matching WITHDRAW payment
        matching_payment = None
        for payment in payments:
            if payment.get("PAYMENTMETHOD") == "WITHDRAW":
                payment_acc_ref = payment.get("ACCOUNT REFERENCE", "") or ""
                if account_reference in payment_acc_ref:
                    matching_payment = payment
                    break

        if not matching_payment:
            return {"success": False, "verified": False, "message": "Withdrawal not found in M-Pesa records yet"}

        transaction_code = matching_payment.get("TRANSACTION CODE", "")
        amount = float(matching_payment.get("AMOUNT", 0))

        # Find and update transaction in subcollections
        user_ref = self.db.collection("TRANSACTIONS").document(user_id)
        for month_collection in user_ref.collections():
            for doc in month_collection.stream():
                data = doc.to_dict() or {}
                tx_acc_ref = data.get("accountReference", "")
                tx_status = data.get("status", "")

                if (
                    tx_acc_ref == account_reference
                    and self._is_withdrawal_transaction(data)
                    and tx_status in {"PENDING", "PROCESSING"}
                ):
                    self._update_withdrawal_transaction_as_done(doc.reference, doc.id, transaction_code)
                    self._update_related_fee_transactions(user_id, doc.id)

                    if account_reference not in self._processed_transactions:
                        self._processed_transactions.append(account_reference)
                        if len(self._processed_transactions) > 100:
                            self._processed_transactions.pop(0)

                    return {
                        "success": True,
                        "verified": True,
                        "transactionCode": transaction_code,
                        "amount": amount,
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                    }

        return {"success": False, "verified": False, "message": "No matching pending withdrawal found in Firestore"}

    def process_withdrawals_for_user(
        self, user_id: str, payments: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """Bulk cron verification for a single user using subcollections."""
        logger.info("Starting withdrawal verification for user: %s", user_id)

        if payments is None:
            payments = self._fetch_payments_from_api()
        if not payments:
            return {"success": False, "error": "Failed to fetch payment data from API"}

        # Filter to only WITHDRAW payments
        withdraw_payments = [p for p in payments if p.get("PAYMENTMETHOD") == "WITHDRAW"]
        if not withdraw_payments:
            return {"success": True, "processedCount": 0, "results": []}

        processed_count = 0
        results: List[Dict[str, Any]] = []

        user_ref = self.db.collection("TRANSACTIONS").document(user_id)

        for month_collection in user_ref.collections():
            for doc in month_collection.stream():
                data = doc.to_dict() or {}
                tx_id = doc.id
                status = data.get("status", "")
                account_reference = data.get("accountReference", "")

                if status not in {"PENDING", "PROCESSING"} or not account_reference:
                    continue

                if not self._is_withdrawal_transaction(data):
                    continue

                if account_reference in self._processed_transactions:
                    continue

                # Match against WITHDRAW payments
                for payment in withdraw_payments:
                    payment_acc_ref = payment.get("ACCOUNT REFERENCE", "") or ""
                    if account_reference in payment_acc_ref:
                        transaction_code = payment.get("TRANSACTION CODE", "")
                        amount = float(payment.get("AMOUNT", 0))

                        self._update_withdrawal_transaction_as_done(doc.reference, tx_id, transaction_code)
                        self._update_related_fee_transactions(user_id, tx_id)

                        self._processed_transactions.append(account_reference)
                        if len(self._processed_transactions) > 100:
                            self._processed_transactions.pop(0)

                        processed_count += 1
                        results.append({
                            "transactionId": tx_id,
                            "transactionCode": transaction_code,
                            "amount": amount,
                        })
                        break

        return {"success": True, "processedCount": processed_count, "results": results}

    def handle_failed_withdrawal_for_user(
        self, user_id: str, account_reference: str, reason: str
    ) -> Dict[str, Any]:
        """Mark a withdrawal as FAILED and reverse balance."""
        logger.info("Handling failed withdrawal: userId=%s ref=%s reason=%s", user_id, account_reference, reason)

        try:
            user_ref = self.db.collection("TRANSACTIONS").document(user_id)

            for month_collection in user_ref.collections():
                for doc in month_collection.stream():
                    data = doc.to_dict() or {}
                    tx_acc_ref = data.get("accountReference", "")
                    tx_status = data.get("status", "")

                    if (
                        tx_acc_ref == account_reference
                        and self._is_withdrawal_transaction(data)
                        and tx_status in {"PENDING", "PROCESSING"}
                    ):
                        account_type = data.get("accountType", "NORMAL")
                        withdrawal_amount = float(data.get("amount", 0))
                        self._failed_withdrawal_processed[account_reference] = True

                        self._update_withdrawal_transaction_as_failed(doc.reference, doc.id, reason)
                        self._update_related_fee_transactions_as_failed(user_id, doc.id)

                        tx_type = data.get("type", "")
                        # if not tx_type.startswith("GROUP_WITHDRAWAL_"):
                        #     self._reverse_withdrawal_deduction(user_id, account_type, withdrawal_amount, doc.id)

                        return {"success": True, "transactionId": doc.id, "amount": withdrawal_amount}

            return {"success": False, "error": "No matching withdrawal found"}
        except Exception as exc:
            logger.exception("Error in handle_failed_withdrawal_for_user: %s", exc)
            return {"success": False, "error": str(exc)}

    def check_for_timed_out_withdrawals_for_user(self, user_id: str) -> Dict[str, Any]:
        """Find withdrawals in PROCESSING > 30 min and mark them FAILED."""
        logger.info("Checking for timed out withdrawals for user: %s", user_id)

        try:
            user_ref = self.db.collection("TRANSACTIONS").document(user_id)
            current_time = int(time.time() * 1000)
            timeout_threshold = 30 * 60 * 1000  # 30 minutes
            timed_out_count = 0

            for month_collection in user_ref.collections():
                for doc in month_collection.stream():
                    data = doc.to_dict() or {}
                    tx_status = data.get("status", "")
                    tx_timestamp = int(data.get("timestamp", 0) or 0)

                    if (
                        self._is_withdrawal_transaction(data)
                        and tx_status == "PROCESSING"
                        and tx_timestamp > 0
                    ):
                        age = current_time - tx_timestamp
                        if age > timeout_threshold:
                            account_reference = data.get("accountReference", "")
                            if account_reference:
                                self.handle_failed_withdrawal_for_user(
                                    user_id, account_reference, "Transaction timed out after 30 minutes"
                                )
                                timed_out_count += 1

            return {"success": True, "timedOutCount": timed_out_count}
        except Exception as exc:
            logger.exception("Error checking for timed out withdrawals: %s", exc)
            return {"success": False, "error": str(exc)}

    @classmethod
    def cleanup_cache(cls) -> None:
        while len(cls._processed_transactions) > 50:
            cls._processed_transactions.pop(0)
        if len(cls._balance_updated_transactions) > 50:
            keys = list(cls._balance_updated_transactions.keys())[:25]
            cls._balance_updated_transactions = {k: cls._balance_updated_transactions[k] for k in keys}
        if len(cls._failed_withdrawal_processed) > 50:
            keys = list(cls._failed_withdrawal_processed.keys())[:25]
            cls._failed_withdrawal_processed = {k: cls._failed_withdrawal_processed[k] for k in keys}
        logger.info(
            "Cache cleanup: processed=%d balance_updated=%d failed=%d",
            len(cls._processed_transactions),
            len(cls._balance_updated_transactions),
            len(cls._failed_withdrawal_processed),
        )
