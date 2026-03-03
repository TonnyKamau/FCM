import unittest
from unittest.mock import MagicMock, patch
from payment_resolver import PaymentResolverService
from mpesa_api import MpesaAPI

class TestPaymentResolver(unittest.TestCase):

    def setUp(self):
        # Mock Firestore and verify services
        self.patcher_db = patch('payment_resolver.get_db')
        self.mock_db = self.patcher_db.start()
        
        self.patcher_payment = patch('payment_resolver.PaymentVerificationService')
        self.mock_payment_service = self.patcher_payment.start().return_value
        
        self.patcher_withdrawal = patch('payment_resolver.WithdrawalVerificationService')
        self.mock_withdrawal_service = self.patcher_withdrawal.start().return_value
        
        self.patcher_mpesa = patch('payment_resolver.MpesaAPI')
        self.mock_mpesa_api = self.patcher_mpesa.start().return_value
        
        self.resolver = PaymentResolverService()

    def tearDown(self):
        self.patcher_db.stop()
        self.patcher_payment.stop()
        self.patcher_withdrawal.stop()
        self.patcher_mpesa.stop()

    def test_stk_success_resolution_with_regex_extraction(self):
        """Verifies that checkoutRequestId is extracted from paymentMethod and resolved immediately."""
        # Mock transaction data with checkoutRequestId inside paymentMethod
        mock_data = {
            "status": "PENDING",
            "lastModified": 1000,
            "nbOfRetries": 0,
            "paymentMethod": "Direct Mobile - CheckoutRequestID: ws_CO_888",
            "accountReference": "ORD789",
            "amount": 200.0,
            "accountType": "NORMAL"
        }
        
        mock_doc = MagicMock()
        mock_doc.id = "tx_003"
        mock_doc.to_dict.return_value = mock_data
        mock_doc.reference.update = MagicMock()
        
        mock_month_col = MagicMock()
        mock_month_col.id = "Feb-2026"
        mock_month_col.where.return_value.stream.return_value = [mock_doc]
        
        mock_user_doc = MagicMock()
        mock_user_doc.id = "user_regex"
        
        self.mock_db.return_value.collection.return_value.list_documents.return_value = [mock_user_doc]
        self.mock_db.return_value.collection.return_value.document.return_value.collections.return_value = [mock_month_col]
        
        # Mock Mpesa API success
        self.mock_mpesa_api.query_stk_push_status.return_value = {"ResultCode": "0", "ResultDesc": "Success"}
        
        # Run resolver
        self.resolver.resolve_now()
        
        # Assertions
        # 1. STK query called with EXTRACTED ID
        self.mock_mpesa_api.query_stk_push_status.assert_called_with("ws_CO_888")
        # 2. Transaction marked DONE directly (not via payment_service proxy anymore)
        self.mock_db.return_value.collection.return_value.document.return_value.collection.return_value.document.return_value.update.assert_called()
        # 3. Balance updated directly
        self.mock_payment_service._update_user_balance.assert_called_with("user_regex", "NORMAL", 200.0)

    def test_stk_failure_resolution(self):
        """Verifies that a failed STK status result marks transaction as FAILED."""
        mock_data = {
            "status": "PENDING",
            "lastModified": 1000,
            "nbOfRetries": 0,
            "paymentMethod": "Direct STK",
            "checkoutRequestId": "ws_CO_123",
            "accountReference": "ORD789"
        }
        
        mock_doc = MagicMock()
        mock_doc.id = "tx_002"
        mock_doc.to_dict.return_value = mock_data
        
        mock_month_col = MagicMock()
        mock_month_col.id = "Feb-2026"
        mock_month_col.where.return_value.stream.return_value = [mock_doc]
        
        mock_user_doc = MagicMock()
        mock_user_doc.id = "user_xyz"
        
        self.mock_db.return_value.collection.return_value.list_documents.return_value = [mock_user_doc]
        self.mock_db.return_value.collection.return_value.document.return_value.collections.return_value = [mock_month_col]
        
        # Mock Mpesa API failure (e.g. Cancelled)
        self.mock_mpesa_api.query_stk_push_status.return_value = {"ResultCode": "1032", "ResultDesc": "Cancelled"}
        
        # Run resolver
        self.resolver.resolve_now()
        
        # Verify status updated to FAILED
        # The internal update call is slightly complex due to chaining, but we expect an update call
        # with "status": "FAILED"
        self.mock_db.return_value.collection.return_value.document.return_value.collection.return_value.document.return_value.update.assert_called()
        args, kwargs = self.mock_db.return_value.collection.return_value.document.return_value.collection.return_value.document.return_value.update.call_args_list[-1]
        self.assertEqual(args[0]["status"], "FAILED")

    def test_b2c_success_triggers_verification(self):
        """Verifies that a successful B2C status query response triggers internal verification."""
        mock_data = {
            "status": "PROCESSING",
            "lastModified": 1000,
            "nbOfRetries": 0,
            "paymentMethod": "M-PESA B2C",
            "accountReference": "B2C_REF_123",
            "amount": 500.0,
            "accountType": "NORMAL",
            "type": "WITHDRAW"
        }
        
        mock_doc = MagicMock()
        mock_doc.id = "tx_b2c_001"
        mock_doc.to_dict.return_value = mock_data
        
        mock_month_col = MagicMock()
        mock_month_col.id = "Feb-2026"
        mock_month_col.where.return_value.stream.return_value = [mock_doc]
        
        mock_user_doc = MagicMock()
        mock_user_doc.id = "user_b2c"
        
        self.mock_db.return_value.collection.return_value.list_documents.return_value = [mock_user_doc]
        self.mock_db.return_value.collection.return_value.document.return_value.collections.return_value = [mock_month_col]
        
        # Mock Mpesa API response 0
        self.mock_mpesa_api.query_transaction_status.return_value = {"ResponseCode": "0", "ResponseDescription": "Success"}
        
        # Mock internal verification success
        self.mock_withdrawal_service.verify_specific_withdrawal.return_value = {"verified": True}
        
        # Run resolver
        self.resolver.resolve_now()
        
        # Assertions
        # 1. Status query called
        self.mock_mpesa_api.query_transaction_status.assert_called()
        # 2. Internal verification service called
        self.mock_withdrawal_service.verify_specific_withdrawal.assert_called_with(
            user_id="user_b2c",
            account_reference="B2C_REF_123",
            account_type="NORMAL",
            withdrawal_amount=500.0,
            is_group_withdrawal=False
        )

if __name__ == '__main__':
    unittest.main()
