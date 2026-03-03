import base64
import requests
import time
import logging
from datetime import datetime
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from config import (
    MPESA_CONSUMER_KEY, MPESA_CONSUMER_SECRET, MPESA_PASSKEY,
    MPESA_BUSINESS_SHORT_CODE, MPESA_INITIATOR_NAME, MPESA_INITIATOR_PASSWORD,
    MPESA_AUTH_URL, MPESA_STK_QUERY_URL, MPESA_TRANSACTION_STATUS_URL,
    MPESA_B2C_RESULT_URL, MPESA_B2C_TIMEOUT_URL
)

logger = logging.getLogger(__name__)

# Safaricom Public Certificate (from Java sample)
SAFARICOM_CERT = """-----BEGIN CERTIFICATE-----
MIIGkzCCBXugAwIBAgIKXfBp5gAAAD+hNjANBgkqhkiG9w0BAQsFADBbMRMwEQYK
CZImiZPyLGQBGRYDbmV0MRkwFwYKCZImiZPyLGQBGRYJc2FmYXJpY29tMSkwJwYD
VQQDEyBTYWZhcmljb20gSW50ZXJuYWwgSXNzdWluZyBDQSAwMjAeFw0xNzA0MjUx
NjA3MjRaFw0xODAzMjExMzIwMTNaMIGNMQswCQYDVQQGEwJLRTEQMA4GA1UECBMH
TmFpcm9iaTEQMA4GA1UEBxMHTmFpcm9iaTEaMBgGA1UEChMRU2FmYXJpY29tIExp
bWl0ZWQxEzARBgNVBAsTClRlY2hub2xvZ3kxKTAnBgNVBAMTIGFwaWdlZS5hcGlj
YWxsZXIuc2FmYXJpY29tLmNvLmtlMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIB
CgKCAQEAoknIb5Tm1hxOVdFsOejAs6veAai32Zv442BLuOGkFKUeCUM2s0K8XEsU
t6BP25rQGNlTCTEqfdtRrym6bt5k0fTDscf0yMCoYzaxTh1mejg8rPO6bD8MJB0c
FWRUeLEyWjMeEPsYVSJFv7T58IdAn7/RhkrpBl1dT7SmIZfNVkIlD35+Cxgab+u7
+c7dHh6mWguEEoE3NbV7Xjl60zbD/Buvmu6i9EYz+27jNVPI6pRXHvp+ajIzTSsi
eD8Ztz1eoC9mphErasAGpMbR1sba9bM6hjw4tyTWnJDz7RdQQmnsW1NfFdYdK0qD
RKUX7SG6rQkBqVhndFve4SDFRq6wvQIDAQABo4IDJDCCAyAwHQYDVR0OBBYEFG2w
ycrgEBPFzPUZVjh8KoJ3EpuyMB8GA1UdIwQYMBaAFOsy1E9+YJo6mCBjug1evuh5
TtUkMIIBOwYDVR0fBIIBMjCCAS4wggEqoIIBJqCCASKGgdZsZGFwOi8vL0NOPVNh
ZmFyaWNvbSUyMEludGVybmFsJTIwSXNzdWluZyUyMENBJTIwMDIsQ049U1ZEVDNJ
U1NDQTAxLENOPUNEUCxDTj1QdWJsaWMlMjBLZXklMjBTZXJ2aWNlcyxDTj1TZXJ2
aWNlcyxDTj1Db25maWd1cmF0aW9uLERDPXNhZmFyaWNvbSxEQz1uZXQ/Y2VydGlm
aWNhdGVSZXZvY2F0aW9uTGlzdD9iYXNlP29iamVjdENsYXNzPWNSTERpc3RyaWJ1
dGlvblBvaW50hkdodHRwOi8vY3JsLnNhZmFyaWNvbS5jby5rZS9TYWZhcmljb20l
MjBJbnRlcm5hbCUyMElzc3VpbmclMjBDQSUyMDAyLmNybDCCAQkGCCsGAQUFBwEB
BIH8MIH5MIHJBggrBgEFBQcwAoaBvGxkYXA6Ly8vQ049U2FmYXJpY29tJTIwSW50
ZXJuYWwlMjBJc3N1aW5nJTIwQ0AlMjAwMixDTj1BSUEsQ049UHVibGljJTIwS2V5
JTIwU2VydmljZXMsQ049U2VydmljZXMsQ049Q29uZmlndXJhdGlvbixEQz1zYWZh
cmljb20sREM9bmV0P2NBQ2VydGlmaWNhdGU/YmFzZT9vYmplY3RDbGFzcz1jZXJ0
aWZpY2F0aW9uQXV0aG9yaXR5MCsGCCsGAQUFBzABhh9odHRwOi8vY3JsLnNhZmFy
aWNvbS5jby5rZS9vY3NwMAsGA1UdDwQEAwIFoDA9BgkrBgEEAYI3FQcEMDAuBiYr
BgEEAYI3FQiHz4xWhMLEA4XphTaE3tENhqCICGeGwcdsg7m5awIBZAIBDDAdBgNV
HSUEFjAUBggrBgEFBQcDAgYIKwYBBQUHAwEwJwYJKwYBBAGCNxUKBBowGDAKBggr
BgEFBQcDAjAKBggrBgEFBQcDATANBgkqhkiG9w0BAQsFAAOCAQEAC/hWx7KTwSYr
x2SOyyHNLTRmCnCJmqxA/Q+IzpW1mGtw4Sb/8jdsoWrDiYLxoKGkgkvmQmB2J3zU
ngzJIM2EeU921vbjLqX9sLWStZbNC2Udk5HEecdpe1AN/ltIoE09ntglUNINyCmf
zChs2maF0Rd/y5hGnMM9bX9ub0sqrkzL3ihfmv4vkXNxYR8k246ZZ8tjQEVsKehE
dqAmj8WYkYdWIHQlkKFP9ba0RJv7aBKb8/KP+qZ5hJip0I5Ey6JJ3wlEWRWUYUKh
gYoPHrJ92ToadnFCCpOlLKWc0xVxANofy6fqreOVboPO0qTAYpoXakmgeRNLUiar
0ah6M/q/KA==
-----END CERTIFICATE-----"""

class MpesaAPI:
    def __init__(self):
        self._access_token = None
        self._token_expiry = 0

    def get_access_token(self):
        """Fetches a new OAuth token or returns valid cached one."""
        if self._access_token and time.time() < self._token_expiry:
            return self._access_token

        auth = base64.b64encode(f"{MPESA_CONSUMER_KEY}:{MPESA_CONSUMER_SECRET}".encode()).decode()
        headers = {"Authorization": f"Basic {auth}"}
        
        try:
            resp = requests.get(MPESA_AUTH_URL, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            self._access_token = data["access_token"]
            # Expiry usually 3600s, buffer by 60s
            self._token_expiry = time.time() + int(data.get("expires_in", 3600)) - 60
            return self._access_token
        except Exception as e:
            logger.error("Failed to get M-Pesa access token: %s", e)
            return None

    def generate_security_credential(self, password):
        """Generates encrypted security credential using Safaricom certificate."""
        try:
            cert = x509.load_pem_x509_certificate(SAFARICOM_CERT.encode())
            public_key = cert.public_key()
            
            encrypted = public_key.encrypt(
                password.encode(),
                padding.PKCS1v15()
            )
            return base64.b64encode(encrypted).decode()
        except Exception as e:
            logger.error("Failed to generate security credential: %s", e)
            return None

    def query_stk_push_status(self, checkout_request_id):
        """Synchronously queries the status of an STK push."""
        token = self.get_access_token()
        if not token:
            return None

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        password_str = MPESA_BUSINESS_SHORT_CODE + MPESA_PASSKEY + timestamp
        password = base64.b64encode(password_str.encode()).decode()

        payload = {
            "BusinessShortCode": MPESA_BUSINESS_SHORT_CODE,
            "Password": password,
            "Timestamp": timestamp,
            "CheckoutRequestID": checkout_request_id
        }

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        try:
            resp = requests.post(MPESA_STK_QUERY_URL, json=payload, headers=headers, timeout=30)
            return resp.json()
        except Exception as e:
            logger.error("STK push query failed: %s", e)
            return None

    def query_transaction_status(self, originator_conversation_id, result_url, timeout_url):
        """Asynchronously queries transaction status."""
        token = self.get_access_token()
        if not token:
            return None

        security_credential = self.generate_security_credential(MPESA_INITIATOR_PASSWORD)
        if not security_credential:
            return None

        payload = {
            "Initiator": MPESA_INITIATOR_NAME,
            "SecurityCredential": security_credential,
            "CommandID": "TransactionStatusQuery",
            "TransactionID": "",
            "OriginalConversationID": originator_conversation_id,
            "PartyA": MPESA_BUSINESS_SHORT_CODE,
            "IdentifierType": "4",
            "ResultURL": result_url,
            "QueueTimeOutURL": timeout_url,
            "Remarks": "Status Query",
            "Occasion": "Reconciliation"
        }

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        try:
            resp = requests.post(MPESA_TRANSACTION_STATUS_URL, json=payload, headers=headers, timeout=30)
            return resp.json()
        except Exception as e:
            logger.error("Transaction status query failed: %s", e)
            return None
