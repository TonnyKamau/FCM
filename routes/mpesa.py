"""
M-Pesa STK push routes for the kit-ifms Flutter POS.

POST /mpesa/stk-push
    Initiates an STK push to the customer's phone.
    Mirrors the Android DirectDepositActivity.performStkPush() flow —
    OAuth token → password → POST to Safaricom processrequest endpoint.

POST /mpesa/stk-query
    Polls Safaricom for the current status of a pending STK push.
    Used by the Flutter MpesaPaymentSheet to poll every 5 s after the
    initial 20-second wait (matching the Android DepositVerificationWorker
    two-phase approach).

POST /mpesa/stk-callback  (public — called by Safaricom)
    Receives the async result from Safaricom after the user completes or
    cancels the M-Pesa prompt.  Logged for audit; Flutter polls via
    /mpesa/stk-query rather than relying on this.
"""

import logging
from flask import Blueprint, request, jsonify
from auth_utils import require_auth
from mpesa_api import MpesaAPI

logger = logging.getLogger(__name__)
mpesa_bp = Blueprint("mpesa", __name__, url_prefix="/mpesa")

_mpesa = MpesaAPI()

# Result codes from Safaricom STK query
_CANCELLED_CODES = {1032, 1037}   # 1032 = user cancelled, 1037 = timed out


# ── STK Push initiation ────────────────────────────────────────────────────────

@mpesa_bp.route("/stk-push", methods=["POST"])
@require_auth
def stk_push():
    """
    Initiate an STK push.

    Request body
    ------------
    {
        "phone":     "0712345678",          // customer phone (any KE format)
        "amount":    1500,                  // integer or float, ceiled to int
        "reference": "POS-1718000000000"    // account reference shown on M-Pesa
    }

    Response  200
    ----------
    {
        "checkoutRequestId": "ws_CO_...",
        "merchantRequestId": "29115-..."
    }
    """
    data = request.get_json(silent=True) or {}

    phone     = str(data.get("phone", "")).strip()
    amount    = data.get("amount")
    reference = str(data.get("reference", "KIT-IFMS POS")).strip()

    if not phone:
        return jsonify({"error": "phone is required"}), 400
    if amount is None:
        return jsonify({"error": "amount is required"}), 400

    try:
        amount = int(float(amount))
        if amount <= 0:
            raise ValueError("amount must be positive")
    except (TypeError, ValueError) as exc:
        return jsonify({"error": f"Invalid amount: {exc}"}), 400

    resp_data, error = _mpesa.stk_push(
        phone=phone,
        amount=amount,
        account_reference=reference,
    )

    if error:
        logger.error("STK push error for phone=%s amount=%s: %s", phone, amount, error)
        return jsonify({"error": f"Failed to initiate payment: {error}"}), 502

    if resp_data is None:
        return jsonify({"error": "No response from M-Pesa"}), 502

    response_code = str(resp_data.get("ResponseCode", "")).strip()
    if response_code != "0":
        error_msg = (
            resp_data.get("errorMessage")
            or resp_data.get("ResponseDescription")
            or "M-Pesa request rejected"
        )
        logger.warning("STK push rejected: %s | raw=%s", error_msg, resp_data)
        return jsonify({"error": error_msg}), 400

    checkout_request_id = resp_data.get("CheckoutRequestID", "")
    merchant_request_id = resp_data.get("MerchantRequestID", "")

    logger.info(
        "STK push sent — phone=%s amount=%s checkoutRequestId=%s merchantRequestId=%s",
        phone, amount, checkout_request_id, merchant_request_id,
    )

    return jsonify({
        "checkoutRequestId": checkout_request_id,
        "merchantRequestId": merchant_request_id,
    }), 200


# ── STK Query (Flutter polls this) ────────────────────────────────────────────

@mpesa_bp.route("/stk-query", methods=["POST"])
@require_auth
def stk_query():
    """
    Query the status of a pending STK push.

    Mirrors the Android DepositVerificationWorker Phase 2 logic — directly
    asking Safaricom via /mpesa/stkpushquery/v1/query.

    Request body
    ------------
    { "checkoutRequestId": "ws_CO_..." }

    Response  200
    -------------
    {
        "verified":   true | false,
        "cancelled":  true | false,
        "resultCode": 0,
        "message":    "The service request is processed successfully."
    }
    """
    data = request.get_json(silent=True) or {}
    checkout_request_id = str(data.get("checkoutRequestId", "")).strip()

    if not checkout_request_id:
        return jsonify({"error": "checkoutRequestId is required"}), 400

    result = _mpesa.query_stk_push_status(checkout_request_id)

    if result is None:
        # Network / token failure — treat as still pending so Flutter keeps polling
        return jsonify({
            "verified":   False,
            "cancelled":  False,
            "resultCode": -1,
            "message":    "Status check failed — will retry",
        }), 200

    result_code = int(result.get("ResultCode", -1))
    result_desc = str(result.get("ResultDesc", "")).strip()

    verified  = result_code == 0
    cancelled = result_code in _CANCELLED_CODES

    logger.info(
        "STK query — checkoutRequestId=%s resultCode=%s verified=%s cancelled=%s",
        checkout_request_id, result_code, verified, cancelled,
    )

    return jsonify({
        "verified":   verified,
        "cancelled":  cancelled,
        "resultCode": result_code,
        "message":    result_desc,
    }), 200


# ── Safaricom async callback (public) ─────────────────────────────────────────

@mpesa_bp.route("/stk-callback", methods=["POST"])
def stk_callback():
    """
    Receives the asynchronous result Safaricom delivers to CallBackURL.
    Flutter does not rely on this endpoint (it polls /stk-query instead),
    but logging the callback provides an audit trail.
    """
    body = request.get_json(silent=True) or {}
    try:
        stk_cb = body.get("Body", {}).get("stkCallback", {})
        result_code = stk_cb.get("ResultCode")
        result_desc = stk_cb.get("ResultDesc", "")
        checkout_id = stk_cb.get("CheckoutRequestID", "")
        merchant_id = stk_cb.get("MerchantRequestID", "")

        if result_code == 0:
            # Extract M-Pesa receipt from callback metadata
            items = stk_cb.get("CallbackMetadata", {}).get("Item", [])
            meta = {item["Name"]: item.get("Value") for item in items}
            logger.info(
                "STK callback SUCCESS — checkoutId=%s merchantId=%s "
                "amount=%s receipt=%s phone=%s",
                checkout_id, merchant_id,
                meta.get("Amount"), meta.get("MpesaReceiptNumber"),
                meta.get("PhoneNumber"),
            )
        else:
            logger.warning(
                "STK callback FAILED — checkoutId=%s code=%s desc=%s",
                checkout_id, result_code, result_desc,
            )
    except Exception as exc:
        logger.exception("Error processing STK callback: %s", exc)

    # Safaricom expects a 200 OK immediately
    return jsonify({"ResultCode": 0, "ResultDesc": "Accepted"}), 200
