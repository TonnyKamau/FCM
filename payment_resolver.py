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
            logger.error(f"STK status query for {tx_id} (CheckoutRequestID: {checkout_request_id}) returned None. Check network or API credentials.")
            return

        # Response Code 0 means Safaricom accepted the query
        # ResultCode 0 means the transaction was successful
        result_code = result.get("ResultCode")
        if result_code is not None:
            result_code = int(result_code)
        
        if result_code == 0:
            logger.info(f"STK tx {tx_id} SUCCESS. Resolving immediately.")
            
            amount = float(data.get("amount", 0))
            account_type = data.get("accountType", "NORMAL")
            merchant_request_id = data.get("accountReference") or checkout_request_id
            
            # 1. Update Transaction to DONE
            self._mark_transaction_done(
                user_id, month, tx_id, "DONE", 
                f"Resolved via Status Query. Result: {result.get('ResultDesc', 'Success')}"
            )
            
            # 2. Update Balance (Directly using internal verification helpers if possible)
            # We use a dummy transaction code since query does not provide one
            self.payment_service._update_user_balance(user_id, account_type, amount)
            
            logger.info(f"STK tx {tx_id} fully resolved and balance updated.")

        elif result_code is not None and result_code > 0:
            logger.warning(f"STK tx {tx_id} FAILED with code {result_code}.")
            self._mark_transaction_done(user_id, month, tx_id, "FAILED", f"M-Pesa result: {result.get('ResultDesc')}")
        else:
            # Still pending or error
            logger.info(f"STK tx {tx_id} still pending or error. Waiting for next pass.")

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
            # Let the existing verification service handle failure/reversal if needed
            logger.info(f"B2C tx {tx_id} max retries hit. Handled by fallback verification.")
            self.withdrawal_service.handle_failed_withdrawal_for_user(
                user_id, data.get("accountReference"), "Max retries exceeded"
            )
        else:
            self._mark_transaction_done(user_id, month, tx_id, "FAILED", "Max retries exceeded")

    def _mark_transaction_done(self, user_id, month, tx_id, status, note):
        update_data = {
            "status": status,
            "lastModified": int(time.time() * 1000),
            "resolverNote": note
        }
        self.db.collection("TRANSACTIONS").document(user_id).collection(month).document(tx_id).update(update_data)
        logger.info(f"Marked tx {tx_id} as {status}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    resolver = PaymentResolverService()
    resolver.resolve_now()
