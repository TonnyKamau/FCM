import logging
import time
from datetime import datetime
from typing import Dict, Any, List
from google.cloud.firestore_v1.base_query import FieldFilter

from firebase_utils import get_db
from mpesa_api import MpesaAPI
from payment_verification_service import PaymentVerificationService
from withdrawal_verification_service import WithdrawalVerificationService
from config import (
    MPESA_B2C_RESULT_URL, MPESA_B2C_TIMEOUT_URL
)

logger = logging.getLogger(__name__)

class PaymentResolverService:
    """
    Python implementation of PendingTransactionResolver.java.
    Proactively checks Safaricom's API for status of transactions.
    """
    
    MIN_AGE_MS = 30_000  # 30 seconds
    MAX_RETRIES = 10

    def __init__(self):
        self.db = get_db()
        self.mpesa_api = MpesaAPI()
        self.payment_service = PaymentVerificationService()
        self.withdrawal_service = WithdrawalVerificationService()

    def resolve_now(self):
        """Main entry point to resolve all pending transactions across all users."""
        logger.info("Starting payment resolution pass...")
        
        # In the backend version, we iterate through all users since it's a cron/service
        users_ref = self.db.collection("TRANSACTIONS")
        for user_doc in users_ref.list_documents():
            user_id = user_doc.id
            self._resolve_for_user(user_id)

    def _resolve_for_user(self, user_id: str):
        """Resolves transactions for a specific user ID."""
        user_ref = self.db.collection("TRANSACTIONS").document(user_id)
        now_ms = int(time.time() * 1000)
        
        for month_collection in user_ref.collections():
            # Query PENDING or PROCESSING transactions
            query = month_collection.where(filter=FieldFilter("status", "in", ["PENDING", "PROCESSING"]))
            docs = query.stream()
            
            for doc in docs:
                data = doc.to_dict()
                tx_id = doc.id
                month = month_collection.id
                
                last_modified = data.get("lastModified", 0)
                retries = data.get("nbOfRetries", 0)
                payment_method = data.get("paymentMethod", "")
                
                # Age gate
                if now_ms - last_modified < self.MIN_AGE_MS:
                    continue
                
                # Retry gate
                if retries >= self.MAX_RETRIES:
                    logger.warning(f"Transaction {tx_id} hit max retries. Marking FAILED.")
                    self._handle_max_retries(user_id, month, tx_id, data)
                    continue

                # Update retry count and timestamp
                doc.reference.update({
                    "nbOfRetries": retries + 1,
                    "lastModified": now_ms
                })

                is_stk_push = payment_method.startswith("Direct")
                
                if is_stk_push:
                    checkout_request_id = data.get("checkoutRequestId")
                    
                    # Fallback: extract from paymentMethod string if missing in field
                    if not checkout_request_id and "CheckoutRequestID: " in payment_method:
                        import re
                        match = re.search(r"CheckoutRequestID:\s*(ws_CO_[\w\d]+)", payment_method)
                        if match:
                            checkout_request_id = match.group(1)
                            logger.info(f"Extracted CheckoutRequestID {checkout_request_id} from paymentMethod.")

                    if not checkout_request_id:
                        logger.info(f"STK tx {tx_id} missing CheckoutRequestID, skipping.")
                        continue
                    self._handle_stk_pending(user_id, month, tx_id, data, checkout_request_id)
                else:
                    self._handle_b2c_pending(user_id, month, tx_id, data)

    def _handle_stk_pending(self, user_id, month, tx_id, data, checkout_request_id):
        """Handles pending STK Push transactions."""
        logger.info(f"Querying STK status for {tx_id} (CheckoutRequestID: {checkout_request_id})...")

        result = self.mpesa_api.query_stk_push_status(checkout_request_id)
        if result is None:
            logger.error(f"STK status query for {tx_id} returned None. Check network or API credentials.")
            return

        try:
            result_code = int(result.get("ResultCode", -1))
        except (TypeError, ValueError):
            result_code = -1

        result_desc = result.get("ResultDesc", "")

        # ── Confirmed paid ────────────────────────────────────────────
        if result_code == 0:
            logger.info(f"STK tx {tx_id} confirmed by Safaricom. Resolving...")

            amount        = float(data.get("amount", 0))
            account_type  = data.get("accountType", "NORMAL")
            merchant_request_id = data.get("accountReference") or checkout_request_id

            # Try PHP verification first to get the real M-Pesa transaction code
            stk_tx_code = f"STK-{checkout_request_id[-10:]}"
            php_result = self.payment_service.verify_and_update_balance(
                merchant_request_id, user_id, account_type, 0, amount
            )
            if php_result.get("verified"):
                stk_tx_code = php_result.get("transactionCode", stk_tx_code)
                logger.info(f"STK tx {tx_id} resolved via PHP — code={stk_tx_code}")
            else:
                # PHP not updated yet — write DONE directly
                self._mark_transaction_done(
                    user_id, month, tx_id, "DONE",
                    extra_fields={
                        "transactionCode": stk_tx_code,
                        "paymentMethod":   "Direct Mobile - Verified",
                    }
                )
                self.payment_service._update_user_balance(user_id, account_type, amount)
                logger.info(f"STK tx {tx_id} resolved directly — code={stk_tx_code}")

        # ── Cancelled by user ─────────────────────────────────────────
        elif result_code == 1032:
            logger.warning(f"STK tx {tx_id} cancelled by user.")
            self._mark_transaction_done(
                user_id, month, tx_id, "FAILED",
                extra_fields={
                    "paymentMethod": "Direct Mobile - Cancelled by user",
                    "failedAt":      int(time.time() * 1000),
                }
            )

        # ── STK push timed out (user did not respond) ─────────────────
        elif result_code == 1037:
            logger.warning(f"STK tx {tx_id} timed out — user did not respond.")
            self._mark_transaction_done(
                user_id, month, tx_id, "FAILED",
                extra_fields={
                    "paymentMethod": "Direct Mobile - STK push request timed out",
                    "failedAt":      int(time.time() * 1000),
                }
            )

        # ── Other definitive failure (result_code > 0) ────────────────
        elif result_code > 0:
            logger.warning(f"STK tx {tx_id} failed — code={result_code} desc={result_desc}")
            self._mark_transaction_done(
                user_id, month, tx_id, "FAILED",
                extra_fields={
                    "paymentMethod": f"Direct Mobile - Failed: {result_desc}",
                    "failedAt":      int(time.time() * 1000),
                }
            )

        # ── Still pending / query error ───────────────────────────────
        else:
            logger.info(f"STK tx {tx_id} still pending (code={result_code}). Waiting for next pass.")

    def _handle_b2c_pending(self, user_id, month, tx_id, data):
        """Handles pending B2C withdrawal transactions."""
        originator_id = data.get("accountReference")
        if not originator_id:
            logger.warning(f"B2C tx {tx_id} missing accountReference/OriginatorID.")
            return

        # For B2C status queries, Safaricom uses the OriginatorConversationID.
        # User confirmed this is usually stored in accountReference.
        # Check if we have a dedicated field first.
        query_id = data.get("originatorConversationId") or originator_id
        
        logger.info(f"Querying B2C status for {tx_id} using ID: {query_id} (Field used: {'originatorConversationId' if data.get('originatorConversationId') else 'accountReference'})...")
        
        # For B2C, Safaricom provides async status query
        result = self.mpesa_api.query_transaction_status(
            query_id, MPESA_B2C_RESULT_URL, MPESA_B2C_TIMEOUT_URL
        )
        
        if result and result.get("ResponseCode") == "0":
            logger.info(f"B2C status query for {tx_id} accepted. Triggering internal verification.")
            
            # Immediately attempt to verify via our own withdrawal service
            # This covers the case where the transaction is actually done but we missed the callback
            amount = float(data.get("amount", 0))
            account_type = data.get("accountType", "NORMAL")
            
            # Note: isGroupWithdrawal and groupId can be inferred or added to data
            is_group = data.get("type", "").startswith("GROUP_WITHDRAWAL")
            
            verify_result = self.withdrawal_service.verify_specific_withdrawal(
                user_id=user_id,
                account_reference=originator_id,
                account_type=account_type,
                withdrawal_amount=amount,
                is_group_withdrawal=is_group
                # group_id might be needed if it's a group withdrawal
            )
            
            if verify_result.get("verified"):
                logger.info(f"B2C tx {tx_id} resolved via internal verification after status query.")
            else:
                logger.info(f"B2C tx {tx_id} status query accepted but not yet found in M-Pesa records: {verify_result.get('message')}")
        else:
            logger.warning(f"B2C status query for {tx_id} failed or rejected: {result}")

    def _handle_max_retries(self, user_id, month, tx_id, data):
        payment_method = data.get("paymentMethod", "")
        is_stk_push = payment_method.startswith("Direct")

        if not is_stk_push:
            # B2C — delegate to the withdrawal service (handles fee reversal etc.)
            logger.info(f"B2C tx {tx_id} max retries hit. Delegating to withdrawal service.")
            self.withdrawal_service.handle_failed_withdrawal_for_user(
                user_id, data.get("accountReference"), "Max retries exceeded"
            )
        else:
            # STK — mark FAILED with proper paymentMethod and failedAt
            logger.warning(f"STK tx {tx_id} max retries exceeded. Marking FAILED.")
            self._mark_transaction_done(
                user_id, month, tx_id, "FAILED",
                extra_fields={
                    "paymentMethod": "Direct Mobile - Verification timed out",
                    "failedAt":      int(time.time() * 1000),
                }
            )

    def _mark_transaction_done(self, user_id, month, tx_id, status, note=None, extra_fields: Dict[str, Any] = None):
        update_data = {
            "status":       status,
            "lastModified": int(time.time() * 1000),
        }
        if note:
            update_data["resolverNote"] = note
        if extra_fields:
            update_data.update(extra_fields)

        self.db.collection("TRANSACTIONS").document(user_id).collection(month).document(tx_id).update(update_data)
        logger.info(f"Marked tx {tx_id} as {status} | fields={list(update_data.keys())}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    resolver = PaymentResolverService()
    resolver.resolve_now()
