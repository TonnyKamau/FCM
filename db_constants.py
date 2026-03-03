"""
Firestore collection names — mirrors both Android Constants.java and
the original Flutter kitifms-windows-main FirestoreConstants exactly.
Import this everywhere instead of hardcoding strings.
"""

# ── Core business collections ─────────────────────────────────────────────────
USERS             = "USER"
ADMINS            = "ADMINS"
PRODUCTS          = "PRODUCTS"
CASH_SALE         = "CASH_SALE"
CREDIT_SALE       = "CREDIT_SALE"
EXPENSES          = "EXPENSES"
CUSTOMERS         = "Customers"
CUSTOMER_PAYMENTS = "CustomerPayments"
STOCK             = "STOCK"
STOCK_OUT         = "STOCK_OUT"
CHATS             = "CHATS"
MESSAGES          = "MESSAGES"
GROUP_ACCOUNTS    = "GroupAccounts"
GROUP_MEMBERS     = "GroupMembers"

# ── Licensing ─────────────────────────────────────────────────────────────────
LICENSES          = "LICENSES"

# ── Collections present in the original project (kitifms-windows-main) ────────
TRANSACTIONS         = "TRANSACTIONS"
TIMELINE             = "TIMELINE"
GROUP_TRANSACTIONS   = "GroupTransactions"
ORDERS               = "ORDERS"
REMINDERS            = "REMINDERS"
LOAN_REQUESTS        = "LOAN_REQUESTS"
EXTENSION_REQUESTS   = "EXTENSION_REQUESTS"
BORROWER_REVIEWS     = "BORROWER_REVIEWS"
KYC_VERIFICATIONS    = "KYC_VERIFICATIONS"

# ── Subcollection names ────────────────────────────────────────────────────────
SALES_SUBCOLLECTION   = "sales"
ENTRIES_SUBCOLLECTION = "entries"

# ── Field names ───────────────────────────────────────────────────────────────
FCM_TOKEN_FIELD   = "currentFCMToken"
