import logging
import time
from typing import Any, Dict, Optional

from mpesa_api import MpesaAPI
from payment_verification_service import PaymentVerificationService
from firebase_utils import get_db

logger = logging.getLogger(__name__)

# Human-readable reason labels stored on the Firestore transaction
_CANCEL_REASONS = {
    1032: "Cancelled by user",
    1037: "STK push request timed out",
}


class StkQueryService:
    """
    Queries Safaricom's STK push status API directly and, when the payment
    is confirmed, updates the Firestore transaction + user balance.

    Called from Android when the PHP verification endpoint has not confirmed
    the transaction after 5 seconds — giving the user a real-time fallback
    that does not depend on the PHP PaymentProcess.php polling cycle.

    Flow
    ----
    1. Query Safaricom: POST /mpesa/stkpushquery/v1/query
    2. ResultCode == 0  → payment confirmed by Safaricom
       a. Try PHP verification first (it may now hold the real M-Pesa code).
       b. If PHP not yet updated, mark the transaction DONE using a
          STK-derived reference and update the user's balance directly.
    3. ResultCode in (1032, 1037) → cancelled / timed-out.
       → Transaction is marked FAILED in Firestore immediately.
    4. Any other code → still pending; Firestore is left unchanged so
       Android can keep polling.
    """

    def __init__(self) -> None:
        self.mpesa = MpesaAPI()
        self.payment_service = PaymentVerificationService()
        self.db = get_db()

    # ------------------------------------------------------------------
    def query_and_verify(
        self,
        checkout_request_id: str,
        merchant_request_id: str,
        user_id: str,
        account_type: str,
        expected_amount: float,
        current_balance: float,
    ) -> Dict[str, Any]:
        """
        Main entry point called by the /stk-query Flask route.

        Parameters
        ----------
        checkout_request_id : str
            The CheckoutRequestID returned by the Safaricom STK push API.
        merchant_request_id : str
            The MerchantRequestID used as accountReference in Firestore.
        user_id : str
            Firestore UID of the depositing user.
        account_type : str
            e.g. "NORMAL" — determines which SAVINGS sub-document to update.
        expected_amount : float
            Amount the user intended to deposit (used as fallback if PHP
            API is not yet updated).
        current_balance : float
            User's balance before the deposit (used only when we fall back
            to updating the balance directly).

        Returns
        -------
        dict with at minimum: success (bool), verified (bool)
        """
        if not checkout_request_id:
            return {"success": False, "error": "checkoutRequestId is required"}

        # ── Step 1: Ask Safaricom for real-time status ─────────────────
        stk_result = self.mpesa.query_stk_push_status(checkout_request_id)

        if stk_result is None:
            return {
                "success": False,
                "verified": False,
                "error": "STK query to Safaricom failed — no response received",
            }

        # ResultCode may arrive as int or string depending on env
        try:
            result_code = int(stk_result.get("ResultCode", -1))
        except (TypeError, ValueError):
            result_code = -1

        result_desc = stk_result.get("ResultDesc", "")

        logger.info(
            "STK query result | checkoutRequestId=%s resultCode=%s desc=%s",
            checkout_request_id,
            result_code,
            result_desc,
        )

        # ── Step 2: Payment confirmed by Safaricom ─────────────────────
        if result_code == 0:
            return self._handle_confirmed_payment(
                checkout_request_id=checkout_request_id,
                merchant_request_id=merchant_request_id,
                user_id=user_id,
                account_type=account_type,
                expected_amount=expected_amount,
                current_balance=current_balance,
                result_code=result_code,
                result_desc=result_desc,
            )

        # ── Step 3: User cancelled (1032) or request timed out (1037) ──
        # Mark the Firestore transaction FAILED immediately so it is never
        # left stuck in PENDING/PROCESSING indefinitely.
        if result_code in (1032, 1037):
            reason = _CANCEL_REASONS.get(result_code, result_desc)
            logger.warning(
                "STK push cancelled/timed-out | checkoutRequestId=%s code=%s reason=%s",
                checkout_request_id,
                result_code,
                reason,
            )
            self._mark_transaction_failed(
                user_id=user_id,
                merchant_request_id=merchant_request_id,
                reason=reason,
            )
            return {
                "success": False,
                "verified": False,
                "cancelled": True,
                "resultCode": result_code,
                "resultDesc": result_desc,
                "reason": reason,
            }

        # ── Step 4: Still pending / unknown ───────────────────────────
        # Leave the Firestore transaction unchanged — Android will keep
        # polling until the 5-minute timeout is reached.
        logger.debug(
            "STK push still pending | checkoutRequestId=%s code=%s",
            checkout_request_id,
            result_code,
        )
        return {
            "success": False,
            "verified": False,
            "pending": True,
            "resultCode": result_code,
            "resultDesc": result_desc,
        }

    # ------------------------------------------------------------------
    def _handle_confirmed_payment(
        self,
        checkout_request_id: str,
        merchant_request_id: str,
        user_id: str,
        account_type: str,
        expected_amount: float,
        current_balance: float,
        result_code: int,
        result_desc: str,
    ) -> Dict[str, Any]:
        """
        Safaricom confirmed the payment (ResultCode=0).

        Attempt A: run the PHP verification pipeline — it may already hold
        the real M-Pesa transaction code if the callback has arrived.

        Attempt B: if PHP is not updated yet, write DONE directly to
        Firestore using a STK-derived reference and update the balance.
        """
        logger.info(
            "Safaricom confirmed payment | checkoutRequestId=%s merchantRequestId=%s",
            checkout_request_id,
            merchant_request_id,
        )

        # ── Attempt A: try PHP verification to get the real Mpesa code ─
        if merchant_request_id:
            php_result = self.payment_service.verify_and_update_balance(
                merchant_request_id,
                user_id,
                account_type,
                current_balance,
                expected_amount,
            )
            if php_result.get("verified"):
                php_result["source"] = "stk_query_confirmed_php_verified"
                php_result["resultCode"] = result_code
                php_result["resultDesc"] = result_desc
                logger.info(
                    "PHP verification succeeded after STK confirmation | "
                    "merchantRequestId=%s transactionCode=%s",
                    merchant_request_id,
                    php_result.get("transactionCode"),
                )
                return php_result

        # ── Attempt B: PHP API not yet updated — write directly ────────
        # Build a deterministic reference from the last 10 chars of the
        # CheckoutRequestID so it can be reconciled later by the cron job.
        stk_tx_code = "STK-{}".format(checkout_request_id[-10:])

        logger.info(
            "PHP verification pending — writing DONE directly | "
            "stk_tx_code=%s userId=%s",
            stk_tx_code,
            user_id,
        )

        lookup_id = merchant_request_id if merchant_request_id else checkout_request_id
        updated = self.payment_service._update_transaction_in_firestore(
            user_id, lookup_id, stk_tx_code
        )

        if not updated:
            # Doc not found OR found but already DONE (status not PENDING/PROCESSING).
            # Check which case: if already DONE another path confirmed it and
            # credited the balance — don't credit again.
            if self.payment_service._transaction_already_confirmed(user_id, lookup_id):
                logger.info(
                    "Transaction already confirmed by another path | lookup_id=%s "
                    "— skipping balance credit to prevent double-crediting",
                    lookup_id,
                )
                return {
                    "success": True,
                    "verified": True,
                    "source": "stk_query_already_confirmed",
                    "transactionCode": stk_tx_code,
                    "resultCode": result_code,
                    "resultDesc": result_desc,
                }

            # True doc-not-found case: Safaricom confirmed the payment but the
            # Firestore doc is missing (race with doc creation, or resolver
            # never wrote it). Must credit the balance — user cannot be left unpaid.
            logger.warning(
                "Firestore transaction not found for lookup_id=%s — "
                "crediting balance directly to ensure accuracy",
                lookup_id,
            )
            balance_result = self.payment_service._update_user_balance(
                user_id, account_type, expected_amount
            )
            return {
                "success": True,
                "verified": True,
                "source": "stk_query_direct_no_doc",
                "transactionCode": stk_tx_code,
                "newBalance": balance_result.get(
                    "newBalance", current_balance + expected_amount
                ),
                "resultCode": result_code,
                "resultDesc": result_desc,
            }

        balance_result = self.payment_service._update_user_balance(
            user_id, account_type, expected_amount
        )

        # Prevent PHP verification loop from re-processing this transaction
        if merchant_request_id:
            self.payment_service._processed_transactions.append(merchant_request_id)

        return {
            "success": True,
            "verified": True,
            "source": "stk_query_direct",
            "transactionCode": stk_tx_code,
            "newBalance": balance_result.get(
                "newBalance", current_balance + expected_amount
            ),
            "resultCode": result_code,
            "resultDesc": result_desc,
        }

    # ------------------------------------------------------------------
    def _mark_transaction_failed(
        self,
        user_id: str,
        merchant_request_id: str,
        reason: str,
    ) -> bool:
        """
        Finds the PENDING/PROCESSING Firestore transaction that matches
        ``merchant_request_id`` and marks it as FAILED.

        Called when Safaricom confirms the STK push was:
          • 1032 — cancelled by the user
          • 1037 — the STK push request timed out before the user responded

        The transaction document is updated with:
          status        → "FAILED"
          paymentMethod → "Direct Mobile - <reason>"
          failedAt      → current timestamp (ms)

        Leaving the transaction as PENDING/PROCESSING would cause the PHP
        cron job and other verifiers to keep retrying it unnecessarily.

        Returns True if the document was found and updated, False otherwise.
        """
        if not user_id or not merchant_request_id:
            logger.warning(
                "_mark_transaction_failed: missing userId or merchantRequestId"
            )
            return False

        try:
            user_ref = self.db.collection("TRANSACTIONS").document(user_id)

            for month_col in user_ref.collections():
                for doc in month_col.stream():
                    data = doc.to_dict() or {}
                    account_ref = data.get("accountReference", "")
                    status      = data.get("status", "")

                    if (
                        account_ref == merchant_request_id
                        and status in {"PENDING", "PROCESSING"}
                    ):
                        doc.reference.update({
                            "status":        "FAILED",
                            "paymentMethod": f"Direct Mobile - {reason}",
                            "failedAt":      int(time.time() * 1000),
                        })
                        logger.info(
                            "Transaction marked FAILED | docId=%s reason=%s",
                            doc.id,
                            reason,
                        )
                        return True

            logger.warning(
                "_mark_transaction_failed: no matching PENDING/PROCESSING "
                "transaction found for merchantRequestId=%s userId=%s",
                merchant_request_id,
                user_id,
            )
            return False

        except Exception as exc:
            logger.exception(
                "_mark_transaction_failed error for userId=%s: %s", user_id, exc
            )
            return False
