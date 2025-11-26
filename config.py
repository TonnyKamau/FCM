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
    "https://kit-ifms.com/node_modules/mobile/PaymentProcess.php",
)

# Shared API token used by backend clients to call verification endpoints.
# Make sure to override this in production using an environment variable.
API_SECRET_TOKEN = os.getenv("API_SECRET_TOKEN", "8b03738b482246e5a5d953f3fa4a03aaa5b5f6520b19587fe997487b935c7199")


