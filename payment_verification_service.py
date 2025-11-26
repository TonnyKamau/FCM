import logging
from typing import Any, Dict, List, Optional, Tuple

import requests

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

    # Simple in-memory cache of processed transaction references
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
        """
        try:
            user_ref = self.db.collection("TRANSACTIONS").document(user_id)
            for month_collection in user_ref.collections():
                for doc in month_collection.stream():
                    data = doc.to_dict() or {}
                    account_ref = data.get("accountReference", "")
                    status = data.get("status", "")

                    if (
                        account_ref == merchant_request_id
                        and status in {"PENDING", "PROCESSING"}
                    ):
                        doc.reference.update(
                            {
                                "status": "DONE",
                                "transactionCode": transaction_code,
                                "paymentMethod": "Direct Mobile - Verified",
                            }
                        )
                        logger.info("Updated transaction %s to DONE", doc.id)
                        return True

            logger.warning(
                "No matching transaction found for merchantRequestId=%s userId=%s",
                merchant_request_id,
                user_id,
            )
            return False
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

            snap = saving_ref.get()
            old_balance = 0.0
            if snap.exists:
                data = snap.to_dict() or {}
                old_balance = float(data.get("amount", 0.0))

            new_balance = old_balance + float(amount)

            saving_ref.set(
                {
                    "id": account_type,
                    "amount": new_balance,
                    "accountType": account_type,
                    "userId": user_id,
                    "lastUpdated": int(__import__("time").time() * 1000),
                }
            )

            logger.info(
                "Updated balance for user=%s accountType=%s: %s -> %s",
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
                "timestamp": __import__("datetime")
                .datetime.utcnow()
                .strftime("%Y-%m-%d %H:%M:%S"),
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

                            doc.reference.update(
                                {
                                    "status": "DONE",
                                    "transactionCode": transaction_code,
                                    "paymentMethod": "Direct Mobile - Verified",
                                }
                            )

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


