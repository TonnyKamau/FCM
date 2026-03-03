from flask import Blueprint, request, jsonify
from firebase_admin import auth as fb_auth
from firebase_utils import get_db
from models import user_to_dict
from auth_utils import (
    create_access_token, create_refresh_token,
    require_api_key, require_refresh_token, require_auth, get_jwt_identity,
)
import requests as req
import config
import db_constants as C
import uuid
from datetime import datetime, timezone

auth_bp = Blueprint("auth", __name__, url_prefix="/auth")

# Firebase Auth REST API — requires the project Web API Key (not the service account)
_FB_AUTH = "https://identitytoolkit.googleapis.com/v1/accounts"


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _firebase_sign_in(email: str, password: str):
    """
    Authenticate email + password through the Firebase Auth REST API.
    Returns (uid, None) on success, (None, error_message) on failure.
    """
    try:
        resp = req.post(
            f"{_FB_AUTH}:signInWithPassword?key={config.FIREBASE_WEB_API_KEY}",
            json={"email": email, "password": password, "returnSecureToken": True},
            timeout=10,
        )
        data = resp.json()
        if resp.status_code == 200:
            return data.get("localId"), None

        code = data.get("error", {}).get("message", "")
        if any(k in code for k in ("EMAIL_NOT_FOUND", "INVALID_PASSWORD", "INVALID_LOGIN_CREDENTIALS")):
            return None, "Invalid email or password"
        if "USER_DISABLED" in code:
            return None, "Account is disabled"
        return None, "Authentication failed. Please try again."
    except Exception as e:
        return None, f"Could not reach authentication service: {e}"


# ── Register ──────────────────────────────────────────────────────────────────

@auth_bp.route("/register", methods=["POST"])
@require_api_key
def register():
    data     = request.get_json() or {}
    name     = data.get("name", "").strip()
    email    = data.get("email", "").strip().lower()
    password = data.get("password", "")

    if not name or not email or not password:
        return jsonify({"error": "name, email and password are required"}), 400
    if len(password) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400

    # Create user in Firebase Auth via Admin SDK
    try:
        fb_user = fb_auth.create_user(
            email=email,
            password=password,
            display_name=name,
        )
        uid = fb_user.uid
    except fb_auth.EmailAlreadyExistsError:
        return jsonify({"error": "An account already exists with this email"}), 409
    except Exception as e:
        return jsonify({"error": f"Registration failed: {e}"}), 500

    # Save full profile data in Firestore (password NOT stored here).
    db   = get_db()
    now  = _now_iso()
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    user_data = {
        "name": name,
        "email": email,
        "phone": "",
        "country_code": "",
        "gender": "",
        "image_url": "",
        "role": "user",
        "is_admin": False,
        "account_number": f"ACC{str(uuid.uuid4().int)[:9]}",
        "is_active": True,
        "is_selected": False,
        C.FCM_TOKEN_FIELD: "",
        "average_rating": 0.0,
        "total_reviews": 0,
        "is_kyc_verified": False,
        "kyc_status": "none",
        "terms_accepted": False,
        "terms_accepted_timestamp": 0,
        "terms_version": "",
        "timestamp": now_ms,
        "borrower_rating": 0.0,
        "borrower_rating_level": "",
        "total_loans": 0,
        "completed_loans": 0,
        "defaulted_loans": 0,
        "on_time_repayment_rate": 0.0,
        "is_guarantor": False,
        "guarantor_rating": 0.0,
        "created_at": now,
        "updated_at": now,
    }
    db.collection(C.USERS).document(uid).set(user_data)

    access_token  = create_access_token(identity=uid)
    refresh_token = create_refresh_token(identity=uid)
    return jsonify({
        "user": user_to_dict(uid, user_data),
        "accessToken": access_token,
        "refreshToken": refresh_token,
    }), 201


# ── Login ─────────────────────────────────────────────────────────────────────

@auth_bp.route("/login", methods=["POST"])
@require_api_key
def login():
    data     = request.get_json() or {}
    email    = data.get("email", "").strip().lower()
    password = data.get("password", "")

    if not email or not password:
        return jsonify({"error": "email and password are required"}), 400

    # Validate credentials against Firebase Auth REST API
    uid, error = _firebase_sign_in(email, password)
    if error:
        return jsonify({"error": error}), 401

    # Fetch Firestore profile
    db  = get_db()
    doc = db.collection(C.USERS).document(uid).get()

    if not doc.exists:
        # Auto-create profile for users added directly in Firebase Console
        now = _now_iso()
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        user_data = {
            "name": email.split("@")[0],
            "email": email,
            "phone": "", "country_code": "", "gender": "",
            "image_url": "", "role": "user", "is_admin": False,
            "account_number": f"ACC{str(uuid.uuid4().int)[:9]}",
            "is_active": True, "is_selected": False,
            C.FCM_TOKEN_FIELD: "",
            "average_rating": 0.0, "total_reviews": 0,
            "is_kyc_verified": False, "kyc_status": "none",
            "terms_accepted": False, "terms_accepted_timestamp": 0,
            "terms_version": "", "timestamp": now_ms,
            "borrower_rating": 0.0, "borrower_rating_level": "",
            "total_loans": 0, "completed_loans": 0,
            "defaulted_loans": 0, "on_time_repayment_rate": 0.0,
            "is_guarantor": False, "guarantor_rating": 0.0,
            "created_at": now, "updated_at": now,
        }
        db.collection(C.USERS).document(uid).set(user_data)
    else:
        user_data = doc.to_dict()
        if not user_data.get("is_active", True):
            return jsonify({"error": "Account is disabled"}), 403

    access_token  = create_access_token(identity=uid)
    refresh_token = create_refresh_token(identity=uid)
    return jsonify({
        "user": user_to_dict(uid, user_data),
        "accessToken": access_token,
        "refreshToken": refresh_token,
    })


# ── Refresh ───────────────────────────────────────────────────────────────────

@auth_bp.route("/refresh", methods=["POST"])
@require_api_key
@require_refresh_token
def refresh():
    uid = get_jwt_identity()
    return jsonify({"accessToken": create_access_token(identity=uid)})


# ── Me ────────────────────────────────────────────────────────────────────────

@auth_bp.route("/me", methods=["GET"])
@require_auth
def me():
    uid = get_jwt_identity()
    db  = get_db()
    doc = db.collection(C.USERS).document(uid).get()
    if not doc.exists:
        return jsonify({"error": "User not found"}), 404
    return jsonify({"user": user_to_dict(doc.id, doc.to_dict())})


# ── Reset Password ────────────────────────────────────────────────────────────

@auth_bp.route("/reset-password", methods=["POST"])
@require_api_key
def reset_password():
    """
    Flutter sends { "email": "..." }.
    Backend generates a Firebase password-reset link and sends a branded email.
    Always returns 200 — never reveals whether the email is registered.
    """
    data  = request.get_json() or {}
    email = data.get("email", "").strip().lower()

    if not email:
        return jsonify({"error": "email is required"}), 400

    try:
        reset_link = fb_auth.generate_password_reset_link(email)

        db   = get_db()
        docs = list(db.collection(C.USERS).where("email", "==", email).limit(1).get())
        recipient_name = docs[0].to_dict().get("name", "User") if docs else "User"

        from utils.email_utils import send_email, _shell
        content = f"""
          <p class="greeting">Hello, {recipient_name}</p>
          <p class="intro">We received a request to reset your <strong>KIT-IFMS</strong> password.
            Click the button below to choose a new password.</p>
          <div style="text-align:center;margin:28px 0;">
            <a href="{reset_link}"
               style="display:inline-block;padding:14px 32px;background:#4F46E5;color:#fff;
                      border-radius:10px;font-size:15px;font-weight:600;text-decoration:none;">
              &#128274; Reset Password
            </a>
          </div>
          <p style="font-size:13px;color:#6B7280;text-align:center;margin-bottom:24px;">
            If the button doesn't work, copy this link into your browser:<br>
            <a href="{reset_link}" style="color:#6366F1;word-break:break-all;">{reset_link}</a>
          </p>
          <div class="info-box amber">
            <p class="info-title">&#9888; Security note</p>
            <ol>
              <li>This link expires in <strong>1 hour</strong></li>
              <li>If you did not request this, you can safely ignore this email</li>
              <li>Your password will not change until you click the link above</li>
            </ol>
          </div>"""

        send_email(
            email, recipient_name,
            "KIT-IFMS \u2014 Reset Your Password",
            _shell(
                "\U0001f512", "Password Reset",
                "Secure link to reset your KIT-IFMS password",
                "#1E3A5F", "#6366F1", content, config.SMTP_SENDER_EMAIL,
            ),
        )
    except Exception:
        pass  # Never reveal whether email exists

    return jsonify({
        "message": "If an account with that email exists, a reset link has been sent."
    })
