"""
Authentication helpers for the kit-ifms routes.

Uses PyJWT (already in requirements.txt) to create and validate the same
JWT tokens as the kit-ifms Flask backend, so the Flutter app works with
existing X-API-Key + Bearer token credentials.
"""

from functools import wraps
from flask import request, jsonify
import jwt as pyjwt
import uuid
from datetime import datetime, timezone, timedelta

from config import JWT_SECRET_KEY, API_KEY

_ACCESS_EXPIRES  = timedelta(days=7)
_REFRESH_EXPIRES = timedelta(days=30)


# ── Token creation ────────────────────────────────────────────────────────────

def create_access_token(identity: str) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub":   identity,
        "iat":   now,
        "nbf":   now,
        "exp":   now + _ACCESS_EXPIRES,
        "type":  "access",
        "fresh": True,
        "jti":   str(uuid.uuid4()),
    }
    return pyjwt.encode(payload, JWT_SECRET_KEY, algorithm="HS256")


def create_refresh_token(identity: str) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub":  identity,
        "iat":  now,
        "nbf":  now,
        "exp":  now + _REFRESH_EXPIRES,
        "type": "refresh",
        "jti":  str(uuid.uuid4()),
    }
    return pyjwt.encode(payload, JWT_SECRET_KEY, algorithm="HS256")


# ── Identity helper ───────────────────────────────────────────────────────────

def get_jwt_identity():
    """Returns the UID from the validated JWT for the current request."""
    return getattr(request, "jwt_identity", None)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _decode_token(token: str, expected_type: str = "access"):
    try:
        payload = pyjwt.decode(
            token, JWT_SECRET_KEY, algorithms=["HS256"],
            options={"verify_exp": True},
        )
        if payload.get("type") != expected_type:
            return None, (jsonify({"error": f"Expected {expected_type} token"}), 401)
        return payload, None
    except pyjwt.ExpiredSignatureError:
        return None, (jsonify({"error": "Token has expired"}), 401)
    except pyjwt.InvalidTokenError as exc:
        return None, (jsonify({"error": "Invalid token", "detail": str(exc)}), 401)


def _bearer() -> str:
    return request.headers.get("Authorization", "").removeprefix("Bearer ").strip()


# ── Decorators ────────────────────────────────────────────────────────────────

def require_api_key(f):
    """Validates X-API-Key header only (no JWT)."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if request.headers.get("X-API-Key", "") != API_KEY:
            return jsonify({"error": "Invalid or missing API key"}), 401
        return f(*args, **kwargs)
    return decorated


def require_auth(f):
    """Validates X-API-Key + JWT access token."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if request.headers.get("X-API-Key", "") != API_KEY:
            return jsonify({"error": "Invalid or missing API key"}), 401
        token = _bearer()
        if not token:
            return jsonify({"error": "Missing authorization token"}), 401
        payload, err = _decode_token(token, "access")
        if err:
            return err
        request.jwt_identity = payload.get("sub") or payload.get("identity", "")
        return f(*args, **kwargs)
    return decorated


def require_refresh_token(f):
    """Validates X-API-Key + JWT refresh token."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if request.headers.get("X-API-Key", "") != API_KEY:
            return jsonify({"error": "Invalid or missing API key"}), 401
        token = _bearer()
        if not token:
            return jsonify({"error": "Missing authorization token"}), 401
        payload, err = _decode_token(token, "refresh")
        if err:
            return err
        request.jwt_identity = payload.get("sub") or payload.get("identity", "")
        return f(*args, **kwargs)
    return decorated


def require_admin(f):
    """
    Validates X-API-Key + JWT + admin role.
    Checks ADMINS/{uid}.role == 'admin'  OR  USERS/{uid}.is_admin == True.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        if request.headers.get("X-API-Key", "") != API_KEY:
            return jsonify({"error": "Invalid or missing API key"}), 401
        token = _bearer()
        if not token:
            return jsonify({"error": "Missing authorization token"}), 401
        payload, err = _decode_token(token, "access")
        if err:
            return err
        uid = payload.get("sub") or payload.get("identity", "")
        request.jwt_identity = uid

        from firebase_utils import get_db
        import db_constants as C
        db = get_db()
        admins_doc = db.collection(C.ADMINS).document(uid).get()
        if admins_doc.exists and admins_doc.to_dict().get("role") == "admin":
            return f(*args, **kwargs)
        user_doc = db.collection(C.USERS).document(uid).get()
        if user_doc.exists and user_doc.to_dict().get("is_admin", False):
            return f(*args, **kwargs)
        return jsonify({"error": "Admin access required"}), 403
    return decorated
