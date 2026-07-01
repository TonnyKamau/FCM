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
USER_CHAT_PREVIEWS = "USER_CHAT_PREVIEWS"
CHATS_SUBCOLLECTION = "CHATS"   # subcollection under USER_CHAT_PREVIEWS/{userId}
MESSAGES_SUBCOLLECTION = "MESSAGES"  # subcollection under CHATS/{chatId}
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

# ── Android BUSINESS_DATA root ───────────────────────────────────────────────
# Android stores all business data under BUSINESS_DATA/{groupId}/{subcollection}
BUSINESS_DATA         = "BUSINESS_DATA"

# Subcollection names used under BUSINESS_DATA/{groupId}/
BD_PRODUCTS           = "products"
BD_SALES              = "sales"
BD_STOCK_MOVEMENTS    = "stock_movements"
BD_CUSTOMERS          = "customers"
BD_CUSTOMER_PAYMENTS  = "customer_payments"

# ── Subcollection names ────────────────────────────────────────────────────────
SALES_SUBCOLLECTION   = "sales"
ENTRIES_SUBCOLLECTION = "entries"

# ── Field names ───────────────────────────────────────────────────────────────
FCM_TOKEN_FIELD   = "currentFCMToken"
