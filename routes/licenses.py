from flask import Blueprint, request, jsonify
from firebase_utils import get_db
from models import license_to_dict
from auth_utils import require_auth, require_admin, get_jwt_identity
import db_constants as C
import uuid
from datetime import datetime, timezone

licenses_bp = Blueprint("licenses", __name__, url_prefix="/licenses")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _try_send_license_email(recipient_email: str, company_name: str,
                            license_key: str, expires_at: str) -> str | None:
    """Send license email. Returns None on success, error string on failure."""
    try:
        from utils.email_utils import send_license_email
        return send_license_email(
            recipient_email=recipient_email,
            recipient_name=company_name,
            license_key=license_key,
            expires_at=expires_at,
        )
    except Exception as e:
        return str(e)


# ── List ──────────────────────────────────────────────────────────────────────

@licenses_bp.route("", methods=["GET"])
@require_admin
def list_licenses():
    db   = get_db()
    docs = db.collection(C.LICENSES).get()
    licenses = sorted(
        [license_to_dict(d.id, d.to_dict()) for d in docs],
        key=lambda l: l.get("issuedAt", ""), reverse=True,
    )
    return jsonify({"licenses": licenses})


# ── Create ────────────────────────────────────────────────────────────────────

@licenses_bp.route("", methods=["POST"])
@require_admin
def create_license():
    data       = request.get_json() or {}
    key        = data.get("key", "").strip()
    expires_at = data.get("expiresAt", "")

    if not key or not expires_at:
        return jsonify({"error": "key and expiresAt are required"}), 400

    db = get_db()
    existing = list(db.collection(C.LICENSES).where("key", "==", key).limit(1).get())
    if existing:
        return jsonify({"error": "License key already exists"}), 409

    license_id      = str(uuid.uuid4())
    recipient_email = data.get("recipientEmail", "").strip()
    company_name    = data.get("companyName", "").strip()

    lic_data = {
        "key":              key,
        "company_name":     company_name,
        "issued_at":        _now_iso(),
        "expires_at":       expires_at,
        "is_used":          False,
        "assigned_to_uid":  "",
        "recipient_email":  recipient_email,
        "used_at":          "",
    }
    db.collection(C.LICENSES).document(license_id).set(lic_data)

    email_error = None
    if recipient_email:
        email_error = _try_send_license_email(
            recipient_email, company_name, key, expires_at,
        )

    resp = {"license": license_to_dict(license_id, lic_data)}
    if email_error:
        resp["emailError"] = email_error
    elif recipient_email:
        resp["emailSent"] = True

    return jsonify(resp), 201


# ── Send / Resend Email ───────────────────────────────────────────────────────

@licenses_bp.route("/<license_id>/send-email", methods=["POST"])
@require_admin
def send_license_email_endpoint(license_id):
    db  = get_db()
    doc = db.collection(C.LICENSES).document(license_id).get()
    if not doc.exists:
        return jsonify({"error": "License not found"}), 404

    lic  = doc.to_dict()
    data = request.get_json() or {}

    recipient_email = (
        data.get("recipientEmail", "").strip()
        or lic.get("recipient_email", "")
    )
    recipient_name = (
        data.get("recipientName", "").strip()
        or lic.get("company_name", "Recipient")
    )

    if not recipient_email:
        return jsonify({"error": "recipientEmail is required"}), 400

    error = _try_send_license_email(
        recipient_email, recipient_name, lic["key"], lic["expires_at"],
    )

    if not error and not lic.get("recipient_email"):
        doc.reference.update({"recipient_email": recipient_email})

    if error:
        return jsonify({"error": f"Email failed: {error}"}), 500
    return jsonify({"message": f"License emailed to {recipient_email}"})


# ── Verify (read-only) ────────────────────────────────────────────────────────

@licenses_bp.route("/verify", methods=["POST"])
@require_auth
def verify_license():
    uid  = get_jwt_identity()
    data = request.get_json() or {}
    key  = data.get("key", "").strip()

    if not key:
        return jsonify({"error": "key is required"}), 400

    db   = get_db()
    docs = list(db.collection(C.LICENSES).where("key", "==", key).limit(1).get())
    if not docs:
        return jsonify({"error": "License not found. Please contact your admin."}), 404

    doc = docs[0]
    lic = doc.to_dict()

    try:
        expires = datetime.fromisoformat(lic["expires_at"].replace("Z", "+00:00"))
        if expires < datetime.now(timezone.utc):
            return jsonify({"error": "License has expired. Please contact your admin."}), 400
    except Exception:
        pass

    if lic.get("is_used") and lic.get("assigned_to_uid") and lic.get("assigned_to_uid") != uid:
        return jsonify({"error": "This license is already in use by another user."}), 400

    return jsonify({"license": license_to_dict(doc.id, lic)})


# ── Use (mark as used) ────────────────────────────────────────────────────────

@licenses_bp.route("/<license_id>/use", methods=["POST"])
@require_auth
def use_license(license_id):
    uid = get_jwt_identity()
    db  = get_db()
    doc = db.collection(C.LICENSES).document(license_id).get()
    if not doc.exists:
        return jsonify({"error": "License not found"}), 404

    lic = doc.to_dict()

    if lic.get("is_used") and lic.get("assigned_to_uid") and lic.get("assigned_to_uid") != uid:
        return jsonify({"error": "This license is already in use by another user."}), 400

    if not lic.get("is_used"):
        doc.reference.update({
            "is_used":         True,
            "assigned_to_uid": uid,
            "used_at":         _now_iso(),
        })
        lic["is_used"]         = True
        lic["assigned_to_uid"] = uid

    return jsonify({"license": license_to_dict(doc.id, lic)})


# ── Update ────────────────────────────────────────────────────────────────────

@licenses_bp.route("/<license_id>", methods=["PUT"])
@require_admin
def update_license(license_id):
    db  = get_db()
    doc = db.collection(C.LICENSES).document(license_id).get()
    if not doc.exists:
        return jsonify({"error": "License not found"}), 404

    data    = request.get_json() or {}
    updates = {}
    if "recipientEmail" in data:
        updates["recipient_email"] = data["recipientEmail"]
    if "companyName" in data:
        updates["company_name"] = data["companyName"]
    if updates:
        doc.reference.update(updates)

    updated = db.collection(C.LICENSES).document(license_id).get()
    return jsonify({"license": license_to_dict(updated.id, updated.to_dict())})


# ── My License ────────────────────────────────────────────────────────────────

@licenses_bp.route("/my-license", methods=["GET"])
@require_auth
def my_license():
    uid = get_jwt_identity()
    db  = get_db()

    doc = None

    # 1. New backend format (snake_case)
    snake_docs = list(
        db.collection(C.LICENSES)
        .where("assigned_to_uid", "==", uid)
        .where("is_used", "==", True)
        .limit(1)
        .get()
    )
    if snake_docs:
        doc = snake_docs[0]

    # 2. Original Flutter format (camelCase)
    if doc is None:
        camel_docs = list(
            db.collection(C.LICENSES)
            .where("assignedToUid", "==", uid)
            .where("isUsed", "==", True)
            .limit(1)
            .get()
        )
        if camel_docs:
            doc = camel_docs[0]

    if doc is None:
        return jsonify({"error": "No license found for this account"}), 404

    lic = doc.to_dict()

    expires_str = lic.get("expires_at") or lic.get("expiresAt", "")
    try:
        expires = datetime.fromisoformat(expires_str.replace("Z", "+00:00"))
        if expires < datetime.now(timezone.utc):
            return jsonify({"error": "License has expired. Please contact your admin."}), 400
    except Exception:
        pass

    return jsonify({"license": license_to_dict(doc.id, lic)})


# ── Admin check ───────────────────────────────────────────────────────────────

@licenses_bp.route("/admin/check", methods=["GET"])
@require_auth
def check_admin():
    uid = get_jwt_identity()
    db  = get_db()

    admins_doc = db.collection(C.ADMINS).document(uid).get()
    if admins_doc.exists:
        role = admins_doc.to_dict().get("role", "")
        if role == "admin":
            return jsonify({"isAdmin": True})

    user_doc = db.collection(C.USERS).document(uid).get()
    is_admin = user_doc.to_dict().get("is_admin", False) if user_doc.exists else False
    return jsonify({"isAdmin": is_admin})
