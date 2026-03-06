import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

from firebase_admin import messaging

from firebase_utils import get_db
from payment_verification_service import PaymentVerificationService

logger = logging.getLogger(__name__)

# Only attempt recovery for FAILED transactions created within this window
_RECOVERY_WINDOW_MS = 24 * 60 * 60 * 1000  # 24 hours


class FailedRecoveryService:
    """
    Scans a user's FAILED Direct Mobile transactions and reconciles them
    against the PHP PaymentProcess.php API.

    Matching strategy
    -----------------
    A FAILED Firestore transaction is matched to a PHP API record when:
      1. PHP record TYPE     == "DEPOSIT"
      2. PHP record AMOUNT   == Firestore transaction amount  (±0.01 tolerance)
      3. PHP record date     == Firestore transaction date    (same calendar day)
      4. PHP TRANSACTION CODE not already used on any DONE transaction

    Called from /recover-failed-transactions (triggered by the Android
    FailedTransactionRecoveryWorker on app start).
    """

    def __init__(self) -> None:
        self.db = get_db()
        self.payment_service = PaymentVerificationService()

    # ------------------------------------------------------------------
    def recover_failed_transactions(self, user_id: str) -> Dict[str, Any]:
        """
        Main entry point.

        Returns
        -------
        { success, recoveredCount, results[] }
        """
        if not user_id:
            return {"success": False, "error": "userId is required"}

        try:
            failed_txs = self._get_failed_transactions(user_id)
            if not failed_txs:
                return {
                    "success": True,
                    "recoveredCount": 0,
                    "message": "No eligible failed transactions",
                }

            php_payments = self.payment_service._fetch_payments_from_api()
            if php_payments is None:
                return {"success": False, "error": "Could not fetch PHP payment records"}

            # Collect TRANSACTION CODEs already on DONE transactions so we
            # never double-credit the same PHP payment to two Firestore docs.
            used_codes: Set[str] = self._get_used_transaction_codes(user_id)

            results: List[Dict[str, Any]] = []
            recovered_count = 0

            for tx in failed_txs:
                match = self._find_matching_payment(
                    tx["amount"], tx["timestamp"], php_payments, used_codes
                )
                if match is None:
                    continue

                tx_code = match.get("TRANSACTION CODE", "")
                used_codes.add(tx_code)  # prevent same code matching another tx

                # Mark FAILED → DONE in Firestore
                self._mark_transaction_done(tx["doc_ref"], tx_code)

                # Credit balance (atomic Firestore Increment)
                self.payment_service._update_user_balance(
                    user_id, tx["accountType"], tx["amount"]
                )

                recovered_count += 1
                results.append(
                    {
                        "transactionId": tx["id"],
                        "amount": tx["amount"],
                        "transactionCode": tx_code,
                        "accountType": tx["accountType"],
                    }
                )
                logger.info(
                    "Recovered FAILED transaction | userId=%s txId=%s amount=%s code=%s",
                    user_id,
                    tx["id"],
                    tx["amount"],
                    tx_code,
                )

            # Send FCM push notification if any transactions were recovered
            if recovered_count > 0:
                self._send_recovery_notification(user_id, results)

            return {
                "success": True,
                "recoveredCount": recovered_count,
                "results": results,
            }

        except Exception as exc:
            logger.exception(
                "Failed transaction recovery error | userId=%s: %s", user_id, exc
            )
            return {"success": False, "error": str(exc)}

    # ------------------------------------------------------------------
    def _get_failed_transactions(self, user_id: str) -> List[Dict[str, Any]]:
        """
        Returns FAILED Direct Mobile transactions created within the last 24 h.
        Only transactions whose paymentMethod starts with "Direct Mobile" are
        eligible — this excludes Paybill/pull transactions handled elsewhere.
        """
        cutoff_ms = int(time.time() * 1000) - _RECOVERY_WINDOW_MS
        failed = []

        user_ref = self.db.collection("TRANSACTIONS").document(user_id)
        for month_col in user_ref.collections():
            for doc in month_col.stream():
                data = doc.to_dict() or {}
                payment_method = data.get("paymentMethod") or ""
                if (
                    data.get("status") == "FAILED"
                    and (
                        payment_method == "Direct Mobile - Request error: timeout"
                        or payment_method.startswith("Direct Mobile - HTTP Error:")
                    )
                    and int(data.get("timestamp", 0)) >= cutoff_ms
                ):
                    failed.append(
                        {
                            "id": doc.id,
                            "doc_ref": doc.reference,
                            "amount": float(data.get("amount", 0)),
                            "timestamp": int(data.get("timestamp", 0)),
                            "accountType": data.get("accountType", "NORMAL"),
                            "mobileNumber": data.get("mobileNumber", ""),
                        }
                    )
        return failed

    def _get_used_transaction_codes(self, user_id: str) -> Set[str]:
        """
        Collect TRANSACTION CODEs already stored on DONE transactions for this
        user, so the same PHP payment is never credited twice.
        """
        codes: Set[str] = set()
        user_ref = self.db.collection("TRANSACTIONS").document(user_id)
        for month_col in user_ref.collections():
            for doc in month_col.stream():
                data = doc.to_dict() or {}
                if data.get("status") == "DONE":
                    code = data.get("transactionCode", "")
                    if code:
                        codes.add(code)
        return codes

    def _find_matching_payment(
        self,
        amount: float,
        timestamp_ms: int,
        php_payments: List[Dict[str, Any]],
        used_codes: Set[str],
    ) -> Optional[Dict[str, Any]]:
        """
        Search the PHP payments list for a record matching:
          - TYPE == "DEPOSIT"
          - AMOUNT matches (±0.01)
          - FORMATTED_DATE is on the same calendar day as the Firestore timestamp
          - TRANSACTION CODE not already used on a DONE transaction
        """
        tx_date = datetime.fromtimestamp(timestamp_ms / 1000).strftime("%Y-%m-%d")

        for payment in php_payments:
            p_type   = (payment.get("TYPE") or "").upper()
            p_amount = float(payment.get("AMOUNT", 0) or 0)
            p_date   = (payment.get("FORMATTED_DATE") or "")[:10]  # "2026-03-06"
            p_code   = payment.get("TRANSACTION CODE", "")

            if (
                p_type == "DEPOSIT"
                and abs(p_amount - amount) < 0.01
                and p_date == tx_date
                and p_code not in used_codes
            ):
                return payment

        return None

    def _mark_transaction_done(self, doc_ref: Any, transaction_code: str) -> None:
        doc_ref.update(
            {
                "status": "DONE",
                "transactionCode": transaction_code,
                "paymentMethod": "Direct Mobile - Recovered",
                "recoveredAt": int(time.time() * 1000),
            }
        )

    def _send_recovery_notification(
        self, user_id: str, results: List[Dict[str, Any]]
    ) -> None:
        """
        Send an FCM push notification to the user for each recovered transaction.
        The FCM token is read from USER/{userId}.fcmToken in Firestore.
        """
        try:
            user_snap = self.db.collection("USER").document(user_id).get()
            if not user_snap.exists:
                return

            fcm_token = (user_snap.to_dict() or {}).get("fcmToken", "")
            if not fcm_token:
                logger.warning(
                    "No FCM token found for userId=%s — skipping notification", user_id
                )
                return

            total = sum(r["amount"] for r in results)
            count = len(results)

            title = "Deposit Confirmed"
            body = (
                f"KES {total:,.0f} deposit has been confirmed and added to your account."
                if count == 1
                else f"{count} deposits totalling KES {total:,.0f} have been confirmed."
            )

            message = messaging.Message(
                notification=messaging.Notification(title=title, body=body),
                token=fcm_token,
            )
            messaging.send(message)
            logger.info(
                "Recovery FCM notification sent | userId=%s total=%s count=%s",
                user_id,
                total,
                count,
            )

        except Exception as exc:
            # Non-critical — balance was already credited; just log and continue
            logger.warning(
                "Failed to send recovery notification for userId=%s: %s", user_id, exc
            )
