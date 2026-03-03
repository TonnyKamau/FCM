from flask import Blueprint, request, jsonify
from firebase_utils import get_db
from models import user_to_dict
from auth_utils import require_auth, require_admin, get_jwt_identity
import db_constants as C
from datetime import datetime, timezone

users_bp = Blueprint("users", __name__, url_prefix="/users")


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


@users_bp.route("/<uid>", methods=["GET"])
@require_auth
def get_user(uid):
    db = get_db()
    doc = db.collection(C.USERS).document(uid).get()
    if not doc.exists:
        return jsonify({"error": "User not found"}), 404
    return jsonify({"user": user_to_dict(doc.id, doc.to_dict())})


@users_bp.route("/<uid>", methods=["PUT"])
@require_auth
def update_user(uid):
    caller = get_jwt_identity()
    db = get_db()
    if caller != uid:
        caller_doc = db.collection(C.USERS).document(caller).get()
        if not caller_doc.exists or not caller_doc.to_dict().get("is_admin", False):
            return jsonify({"error": "Forbidden"}), 403

    doc = db.collection(C.USERS).document(uid).get()
    if not doc.exists:
        return jsonify({"error": "User not found"}), 404

    data = request.get_json() or {}
    updates = {"updated_at": _now_iso()}

    field_map = {
        "name": "name",
        # Accept both 'phone' and 'phoneNum' — Flutter UserModel sends 'phoneNum'
        "phoneNum": "phone",
        "phone": "phone",
        "countryCode": "country_code",
        "gender": "gender",
        # Accept both 'image' and 'photoUrl' as the profile picture
        "image": "image_url",
        "photoUrl": "image_url",
        "fcmToken": C.FCM_TOKEN_FIELD,
        "currentFCMToken": C.FCM_TOKEN_FIELD,
    }
    for req_key, db_key in field_map.items():
        if req_key in data:
            updates[db_key] = data[req_key]

    doc.reference.update(updates)
    updated = db.collection(C.USERS).document(uid).get()
    return jsonify({"user": user_to_dict(updated.id, updated.to_dict())})


@users_bp.route("/<uid>", methods=["DELETE"])
@require_admin
def delete_user(uid):
    db = get_db()
    doc = db.collection(C.USERS).document(uid).get()
    if not doc.exists:
        return jsonify({"error": "User not found"}), 404
    doc.reference.delete()
    return jsonify({"message": "User deleted"})


@users_bp.route("/<uid>/fcm-token", methods=["PUT"])
@require_auth
def update_fcm_token(uid):
    caller = get_jwt_identity()
    if caller != uid:
        return jsonify({"error": "Forbidden"}), 403
    data = request.get_json() or {}
    token = data.get("fcmToken", "")
    db = get_db()
    doc = db.collection(C.USERS).document(uid).get()
    if not doc.exists:
        return jsonify({"error": "User not found"}), 404
    doc.reference.update({C.FCM_TOKEN_FIELD: token, "updated_at": _now_iso()})
    return jsonify({"message": "FCM token updated"})
