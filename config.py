import os

# Path to your Firebase service account JSON
# In this project it lives in the root as serviceAccountKey.json
FIREBASE_CREDENTIALS_PATH = os.getenv(
    "FIREBASE_CREDENTIALS_PATH",
    os.path.join(os.path.dirname(__file__), "serviceAccountKey.json"),
)

# External payments API used to verify STK/M-Pesa transactions.
# Match the original PHP PAYMENT_API_URL.
PAYMENT_API_URL = os.getenv(
    "PAYMENT_API_URL",
    "https://kit-ifms.com/node_modules/online/PaymentProcess.php",
)

# Pull API for Direct Mobile transactions
PULL_API_URL = os.getenv(
    "PULL_API_URL",
    "https://kit-ifms.com/node_modules/online/pull_query.php",
)

# Shared API token used by backend clients to call verification endpoints.
# Make sure to override this in production using an environment variable.
API_SECRET_TOKEN = os.getenv("API_SECRET_TOKEN", "8b03738b482246e5a5d953f3fa4a03aaa5b5f6520b19587fe997487b935c7199")

# ── kit-ifms app integration ───────────────────────────────────────────────────
# JWT secret — must match JWT_SECRET_KEY on the kit-ifms backend.
# Set this in the PythonAnywhere Web tab → Environment variables.
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "change-me-jwt-secret-in-production")

# API key — must match API_KEY in the Flutter app's .env file.
API_KEY = os.getenv("API_KEY", "kitifms-api-key-change-in-production")

# Firebase Web API Key — Firebase Console → Project Settings → General → Web API Key.
# Required for Auth REST API (email/password sign-in + password reset).
FIREBASE_WEB_API_KEY = os.getenv("FIREBASE_WEB_API_KEY", "")

# ── SMTP (license emails + password reset) ────────────────────────────────────
SMTP_HOST            = os.getenv("SMTP_HOST",            "smtp.gmail.com")
SMTP_PORT            = int(os.getenv("SMTP_PORT",        "587"))
SMTP_SENDER_EMAIL    = os.getenv("SMTP_SENDER_EMAIL",    "")
SMTP_SENDER_PASSWORD = os.getenv("SMTP_SENDER_PASSWORD", "")
SMTP_SENDER_NAME     = os.getenv("SMTP_SENDER_NAME",     "KIT-IFMS")

# Safaricom M-Pesa API Credentials
MPESA_CONSUMER_KEY = os.getenv("MPESA_CONSUMER_KEY", "PK7v1atOPyzLj2ph4w8dbs0Rlh60AzwZAMyoBot2s3Q0Xg4w")
MPESA_CONSUMER_SECRET = os.getenv("MPESA_CONSUMER_SECRET", "jie8oMbSxpdHYnCyDReVY1oFmByltK8KAN4AdtSahAayBJw4oQaKgmBUxJZlutDK")
MPESA_PASSKEY = os.getenv("MPESA_PASSKEY", "7fcac6fe2c315261315c5d7424978f2d3cbe506bbeaf5f57fb1f711a4f32b31f")
MPESA_BUSINESS_SHORT_CODE = os.getenv("MPESA_BUSINESS_SHORT_CODE", "4136651")
MPESA_INITIATOR_NAME = os.getenv("MPESA_INITIATOR_NAME", "Ambwere")
MPESA_INITIATOR_PASSWORD = os.getenv("MPESA_INITIATOR_PASSWORD", "Ambwere2024!")

# M-Pesa API Endpoints
MPESA_BASE_URL = "https://api.safaricom.co.ke/"
MPESA_AUTH_URL = MPESA_BASE_URL + "oauth/v1/generate?grant_type=client_credentials"
MPESA_STK_QUERY_URL = MPESA_BASE_URL + "mpesa/stkpushquery/v1/query"
MPESA_TRANSACTION_STATUS_URL = MPESA_BASE_URL + "mpesa/transactionstatus/v1/query"

# Callback URLs (from Java/Local config)
MPESA_B2C_RESULT_URL = "https://kit-ifms.com/node_modules/online/b2c_result.php"
MPESA_B2C_TIMEOUT_URL = "https://kit-ifms.com/node_modules/online/b2c_time_out.php"
