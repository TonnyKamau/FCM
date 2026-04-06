import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from google.cloud import firestore as fs

from config import PAYMENT_API_URL
from firebase_utils import get_db


logger = logging.getLogger(__name__)


class PaymentVerificationService:
    """
    Python port of the original PHP PaymentVerificationService.

    Firestore structure (must match existing data created by the PHP app):
      - TRANSACTIONS/{userId}/{monthCollection}/{transactionId}
      - SAVINGS/{userId}/accounts/{accountType}
    """

    # Simple in-memory cache of processed transaction references.
    # WARNING: This cache is per-process. In a multi-worker deployment (e.g.
    # gunicorn --workers N) each worker maintains its own copy, so this list
    # provides NO cross-worker deduplication protection.
    # The primary double-credit guard is the atomic Firestore transaction inside
    # _update_transaction_in_firestore(). This cache is only a cheap fast-path
    # optimisation for the *same* worker within a single running session.
    # For a persistent, cross-worker solution replace this list with a Firestore
    # document or a Redis SET keyed on merchant_request_id.
    _processed_transactions: List[str] = []

    def __init__(self) -> None:
        self.db = get_db()
        self.payment_api_url = PAYMENT_API_URL
        logger.info("PaymentVerificationService initialized")

    # -------- Internal helpers -------------------------------------------------
    def _fetch_payments_from_api(self) -> Optional[List[Dict[str, Any]]]:
        try:
            # Some hosts (with ModSecurity) block the default python-requests
            # user agent. Mimic a generic curl request, similar to PHP cURL.
            headers = {
                "User-Agent": "curl/7.81.0",
                "Accept": "*/*",
            }
            resp = requests.get(self.payment_api_url, timeout=30, headers=headers)
        except Exception as exc:  # noqa: BLE001
            logger.error("Payment API request failed: %s", exc)
            return None

        if resp.status_code != 200:
            logger.error(
                "Payment API returned HTTP %s: %s",
                resp.status_code,
                resp.text[:500],
            )
            return None

        try:
            data = resp.json()
        except ValueError as exc:  # JSON decode error
            logger.error("Failed to decode payment API JSON: %s", exc)
            return None

        if not isinstance(data, list):
            logger.error("Unexpected payment API payload type: %s", type(data))
            return None

        return data

    def _update_transaction_in_firestore(
        self,
        user_id: str,
        merchant_request_id: str,
        transaction_code: str,
    ) -> bool:
        """
        Locate a PENDING/PROCESSING transaction whose accountReference matches the
        given merchant_request_id, and mark it as DONE plus attach transactionCode.

        Uses a Firestore transaction to atomically read-then-write so that two
        concurrent callers (e.g. two gunicorn workers or /stk-query + cron) cannot
        both observe PENDING and both return True, which would cause a double
        balance credit downstream.
        """
        try:
            user_ref = self.db.collection("TRANSACTIONS").document(user_id)

            # Collect candidate doc refs first (outside the transaction), then
            # confirm+update inside an atomic Firestore transaction.
            target_ref: Optional[Any] = None
            for month_collection in user_ref.collections():
                for doc in month_collection.stream():
                    data = doc.to_dict() or {}
                    if (
                        data.get("accountReference", "") == merchant_request_id
                        and data.get("status", "") in {"PENDING", "PROCESSING"}
                    ):
                        target_ref = doc.reference
                        break
                if target_ref:
                    break

            if target_ref is None:
                logger.warning(
                    "No matching PENDING/PROCESSING transaction found for "
                    "merchantRequestId=%s userId=%s",
                    merchant_request_id,
                    user_id,
                )
                return False

            # Atomic read-check-write: only one concurrent caller can win.
            @fs.transactional
            def _do_update(transaction: Any, ref: Any) -> bool:
                snap = ref.get(transaction=transaction)
                if not snap.exists:
                    return False
                current_status = (snap.to_dict() or {}).get("status", "")
                if current_status not in {"PENDING", "PROCESSING"}:
                    # Another worker already handled this transaction.
                    logger.info(
                        "Transaction %s already in status=%s — skipping (concurrent update)",
                        snap.id,
                        current_status,
                    )
                    return False
                transaction.update(
                    ref,
                    {
                        "status": "DONE",
                        "transactionCode": transaction_code,
                        "paymentMethod": "Direct Mobile - Verified",
                    },
                )
                logger.info("Atomically updated transaction %s to DONE", snap.id)
                return True

            txn = self.db.transaction()
            result = _do_update(txn, target_ref)
            return result

        except Exception as exc:  # noqa: BLE001
            logger.exception("Firestore transaction update error: %s", exc)
            return False

    def _update_user_balance(
        self,
        user_id: str,
        account_type: str,
        amount: float,
    ) -> Dict[str, Any]:
        try:
            saving_ref = (
                self.db.collection("SAVINGS")
                .document(user_id)
                .collection("accounts")
                .document(account_type)
            )

            # Read old balance for logging/response only — not used in the write
            snap = saving_ref.get()
            old_balance = 0.0
            if snap.exists:
                data = snap.to_dict() or {}
                old_balance = float(data.get("amount", 0.0))

            # Atomic server-side increment — safe against concurrent updates
            # (e.g. STK query + PHP callback confirming simultaneously)
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
        except Exception as exc:  # noqa: BLE001
            logger.exception("Balance update error: %s", exc)
            return {"success": False, "error": str(exc)}

    def _transaction_already_confirmed(
        self, user_id: str, account_reference: str
    ) -> bool:
        """
        Return True if a DONE transaction matching account_reference already
        exists for this user.

        Called before the stk_query_direct_no_doc balance credit to prevent
        double-crediting when payment_resolver or another path confirmed first.
        """
        try:
            user_ref = self.db.collection("TRANSACTIONS").document(user_id)
            for month_col in user_ref.collections():
                for doc in month_col.stream():
                    data = doc.to_dict() or {}
                    if (
                        data.get("accountReference", "") == account_reference
                        and data.get("status", "") == "DONE"
                    ):
                        return True
            return False
        except Exception as exc:  # noqa: BLE001
            logger.exception("Error checking if transaction already confirmed: %s", exc)
            return False

    # -------- Public API (mirrors PHP version) ---------------------------------
    def verify_and_update_balance(
        self,
        merchant_request_id: str,
        user_id: str,
        account_type: str,
        current_balance: float,
        expected_amount: float,
    ) -> Dict[str, Any]:
        try:
            if merchant_request_id in self._processed_transactions:
                return {
                    "success": True,
                    "verified": True,
                    "message": "Already processed",
                    "cached": True,
                }

            payments = self._fetch_payments_from_api()
            if payments is None:
                return {
                    "success": False,
                    "error": "Failed to fetch payment data from M-Pesa",
                }

            matching_payment: Optional[Dict[str, Any]] = None
            for payment in payments:
                account_reference = payment.get("ACCOUNT REFERENCE", "") or ""
                payment_method = payment.get("PAYMENTMETHOD", "") or ""

                if merchant_request_id in account_reference and payment_method == "STK":
                    matching_payment = payment
                    break

            if matching_payment is None:
                return {
                    "success": False,
                    "verified": False,
                    "message": "Payment not found in M-Pesa records yet",
                }

            transaction_code = matching_payment.get("TRANSACTION CODE", "")
            amount = float(matching_payment.get("AMOUNT", 0.0))

            updated = self._update_transaction_in_firestore(
                user_id, merchant_request_id, transaction_code
            )
            if not updated:
                return {
                    "success": False,
                    "error": "Failed to find or update transaction in database",
                }

            balance_result = self._update_user_balance(user_id, account_type, amount)
            if not balance_result.get("success"):
                return {
                    "success": False,
                    "error": f"Failed to update balance: {balance_result.get('error')}",
                }

            self._processed_transactions.append(merchant_request_id)

            return {
                "success": True,
                "verified": True,
                "transactionCode": transaction_code,
                "amount": amount,
                "oldBalance": balance_result["oldBalance"],
                "newBalance": balance_result["newBalance"],
                "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            }
        except Exception as exc:  # noqa: BLE001
            logger.exception("Verification error: %s", exc)
            return {"success": False, "error": f"System error: {exc}"}

    def process_pending_transactions(self, user_id: str) -> Dict[str, Any]:
        """
        Bulk verification equivalent to the PHP processPendingTransactions().
        """
        try:
            user_ref = self.db.collection("TRANSACTIONS").document(user_id)
            user_snap = user_ref.get()
            if not user_snap.exists:
                return {
                    "success": True,
                    "verifiedCount": 0,
                    "message": "No transactions found",
                }

            payments = self._fetch_payments_from_api()
            if payments is None:
                return {"success": False, "error": "Failed to fetch payment data"}

            verified_count = 0
            results: List[Dict[str, Any]] = []

            for month_collection in user_ref.collections():
                for doc in month_collection.stream():
                    data = doc.to_dict() or {}
                    transaction_id = doc.id
                    status = data.get("status", "")
                    account_reference = data.get("accountReference", "")
                    account_type = data.get("accountType", "normal")

                    if status not in {"PENDING", "PROCESSING"} or not account_reference:
                        continue

                    if account_reference in self._processed_transactions:
                        continue

                    for payment in payments:
                        payment_account_ref = payment.get("ACCOUNT REFERENCE", "") or ""
                        payment_method = payment.get("PAYMENTMETHOD", "") or ""

                        if (
                            account_reference in payment_account_ref
                            and payment_method == "STK"
                        ):
                            transaction_code = payment.get("TRANSACTION CODE", "")
                            amount = float(payment.get("AMOUNT", 0.0))

                            # Use the atomic helper so concurrent callers (workers /
                            # endpoints) cannot both win and double-credit the balance.
                            updated = self._update_transaction_in_firestore(
                                user_id, account_reference, transaction_code
                            )
                            if not updated:
                                # Another path already confirmed this transaction.
                                logger.info(
                                    "process_pending_transactions: tx %s already "
                                    "confirmed — skipping balance credit",
                                    transaction_id,
                                )
                                self._processed_transactions.append(account_reference)
                                break

                            balance_result = self._update_user_balance(
                                user_id, account_type, amount
                            )

                            self._processed_transactions.append(account_reference)
                            verified_count += 1

                            results.append(
                                {
                                    "transactionId": transaction_id,
                                    "transactionCode": transaction_code,
                                    "amount": amount,
                                    "newBalance": balance_result.get("newBalance", 0),
                                }
                            )
                            break

            return {
                "success": True,
                "verifiedCount": verified_count,
                "results": results,
            }
        except Exception as exc:  # noqa: BLE001
            logger.exception("Process pending error: %s", exc)
            return {"success": False, "error": str(exc)}


