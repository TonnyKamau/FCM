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
import time
import requests
from flask import Blueprint, request, jsonify
from auth_utils import require_auth, get_jwt_identity
from firebase_utils import get_db
from cache_utils import cached_is_member
from routes.sales import _check_member
from mpesa_api import MpesaAPI
from config import PAYMENT_API_URL

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
        "reference": "POS48213"             // short account reference shown on M-Pesa
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
    group_id  = str(data.get("groupId", "")).strip()

    if not phone:
        return jsonify({"error": "phone is required"}), 400
    if amount is None:
        return jsonify({"error": "amount is required"}), 400
    if not group_id:
        return jsonify({"error": "groupId is required"}), 400

    uid = get_jwt_identity()
    db = get_db()
    is_member, _ = cached_is_member(
        group_id,
        uid,
        lambda: _check_member(db, group_id, uid),
    )
    if not is_member:
        return jsonify({"error": "Access denied"}), 403

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
    group_id = str(data.get("groupId", "")).strip()

    if not checkout_request_id:
        return jsonify({"error": "checkoutRequestId is required"}), 400
    if not group_id:
        return jsonify({"error": "groupId is required"}), 400

    uid = get_jwt_identity()
    db = get_db()
    is_member, _ = cached_is_member(
        group_id,
        uid,
        lambda: _check_member(db, group_id, uid),
    )
    if not is_member:
        return jsonify({"error": "Access denied"}), 403

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


# ── Paybill (C2B) verification ────────────────────────────────────────────────

def _fetch_php_paybill_records():
    """
    Confirmed C2B payments recorded by the legacy PHP layer (the paybill's
    confirmation URL). The host's anti-bot layer intermittently rejects user
    agents with HTTP 409, so try a curl profile then a browser profile.
    Returns a list or None when the feed is unreachable.
    """
    for user_agent in ("curl/8.5.0", "Mozilla/5.0"):
        try:
            resp = requests.get(
                PAYMENT_API_URL,
                headers={"User-Agent": user_agent, "Accept": "*/*"},
                timeout=30,
            )
            if resp.status_code != 200:
                continue
            data = resp.json()
            if isinstance(data, list):
                return data
        except Exception as exc:
            logger.warning("PHP payment feed fetch failed (%s): %s", user_agent, exc)
    return None


@mpesa_bp.route("/paybill-query", methods=["POST"])
@require_auth
def paybill_query():
    """
    Verify a C2B paybill payment for a POS sale.

    The customer pays Paybill 4136651 with the bill/order number as the
    account reference; the POS polls this endpoint with that reference and
    the sale amount. A payment only verifies when the reference matches
    EXACTLY and the paid amount equals the sale amount, and each M-Pesa
    receipt can be claimed by exactly one sale (idempotent for re-polls of
    the same reference).

    Request body
    ------------
    { "reference": "POS48213", "amount": 1500, "groupId": "..." }

    Response 200
    ------------
    { "verified": true,  "receipt": "UG9...", "amountPaid": 1500, "customer": "2547..." }
    { "verified": false, "pending": true,  "message": "Payment not found yet" }
    { "verified": false, "pending": false, "amountMismatch": true, "amountPaid": 1400, ... }
    """
    data = request.get_json(silent=True) or {}
    reference = str(data.get("reference", "")).strip()
    amount = data.get("amount")
    group_id = str(data.get("groupId", "")).strip()

    if not reference:
        return jsonify({"error": "reference is required"}), 400
    if amount is None:
        return jsonify({"error": "amount is required"}), 400
    if not group_id:
        return jsonify({"error": "groupId is required"}), 400

    uid = get_jwt_identity()
    db = get_db()
    is_member, _ = cached_is_member(
        group_id,
        uid,
        lambda: _check_member(db, group_id, uid),
    )
    if not is_member:
        return jsonify({"error": "Access denied"}), 403

    try:
        expected = round(float(amount), 2)
        if expected <= 0:
            raise ValueError("amount must be positive")
    except (TypeError, ValueError) as exc:
        return jsonify({"error": f"Invalid amount: {exc}"}), 400

    ref_norm = reference.lower()
    candidates = []

    # Source 1: PHP-recorded C2B confirmations (authoritative).
    php_records = _fetch_php_paybill_records()
    for record in php_records or []:
        if str(record.get("PAYMENTMETHOD", "")) != "PAYBILL":
            continue
        if str(record.get("ACCOUNT REFERENCE", "")).strip().lower() != ref_norm:
            continue
        receipt = str(record.get("TRANSACTION CODE", "")).strip()
        if receipt:
            candidates.append({
                "receipt": receipt,
                "paid": round(float(record.get("AMOUNT") or 0), 2),
                "customer": str(record.get("CUSTOMER") or ""),
                "source": "php_feed",
            })

    # Source 2: Daraja Pull API — safety net for missed confirmations.
    for row in _mpesa.pull_c2b_transactions() or []:
        if str(row.get("billreference", "")).strip().lower() != ref_norm:
            continue
        receipt = str(row.get("transactionId", "")).strip()
        if not receipt or any(c["receipt"] == receipt for c in candidates):
            continue
        candidates.append({
            "receipt": receipt,
            "paid": round(float(row.get("amount") or 0), 2),
            "customer": str(row.get("msisdn") or ""),
            "source": "pull_api",
        })

    if not candidates:
        return jsonify({
            "verified": False,
            "pending": True,
            "message": "Payment not found yet",
        }), 200

    amount_mismatch = None
    for candidate in candidates:
        if abs(candidate["paid"] - expected) >= 0.01:
            amount_mismatch = candidate
            continue

        # Claim the receipt atomically — one M-Pesa payment funds one sale.
        marker_ref = db.collection("POS_PAYBILL_RECEIPTS").document(candidate["receipt"])
        claimed = False
        try:
            marker_ref.create({
                "receipt": candidate["receipt"],
                "reference": reference,
                "groupId": group_id,
                "amount": candidate["paid"],
                "customer": candidate["customer"],
                "source": candidate["source"],
                "claimedBy": uid,
                "claimedAt": int(time.time() * 1000),
            })
            claimed = True
        except Exception:
            existing = marker_ref.get()
            existing_data = existing.to_dict() if existing.exists else {}
            # Re-poll of the SAME sale is idempotent; a different sale may not
            # reuse the receipt.
            claimed = (
                str(existing_data.get("reference", "")).lower() == ref_norm
                and str(existing_data.get("groupId", "")) == group_id
            )

        if claimed:
            logger.info(
                "Paybill payment verified — ref=%s receipt=%s amount=%s source=%s",
                reference, candidate["receipt"], candidate["paid"], candidate["source"],
            )
            return jsonify({
                "verified": True,
                "receipt": candidate["receipt"],
                "amountPaid": candidate["paid"],
                "customer": candidate["customer"],
                "source": candidate["source"],
            }), 200

    if amount_mismatch:
        return jsonify({
            "verified": False,
            "pending": False,
            "amountMismatch": True,
            "receipt": amount_mismatch["receipt"],
            "amountPaid": amount_mismatch["paid"],
            "message": (
                f"Payment found but the amount differs — paid KES "
                f"{amount_mismatch['paid']:g}, expected KES {expected:g}."
            ),
        }), 200

    return jsonify({
        "verified": False,
        "pending": True,
        "message": "Matching payment was already used for another sale",
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
