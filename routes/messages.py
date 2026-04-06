from flask import Blueprint, request, jsonify
from google.cloud import firestore as fs

from firebase_utils import get_db
from models import message_to_dict
from auth_utils import require_auth, get_jwt_identity
import db_constants as C
import uuid
from datetime import datetime, timezone

messages_bp = Blueprint("messages", __name__, url_prefix="/groups/<group_id>/messages")

_DEFAULT_LIMIT = 100
_MAX_LIMIT = 200


def _check_member(db, group_id, uid):
    """Returns (is_member: bool, group_data: dict|None)."""
    doc = db.collection(C.GROUP_ACCOUNTS).document(group_id).get()
    if doc.exists:
        gd = doc.to_dict()
        if gd.get("admin_id") == uid:
            return True, gd
        gm = list(
            db.collection(C.GROUP_MEMBERS)
            .where("group_id", "==", group_id)
            .where("user_id", "==", uid)
            .limit(1).get()
        )
        if gm:
            return True, gd
    try:
        chats_doc = db.collection(C.CHATS).document(uid).get()
        if chats_doc.exists:
            chat_data = chats_doc.to_dict() or {}
            if group_id in chat_data and isinstance(chat_data[group_id], dict):
                return True, chat_data[group_id]
    except Exception:
        pass
    return False, None


def _clamp_limit(raw_limit):
    try:
        limit = int(raw_limit or _DEFAULT_LIMIT)
    except (TypeError, ValueError):
        limit = _DEFAULT_LIMIT
    return max(1, min(limit, _MAX_LIMIT))


def _is_true(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _load_legacy_messages(db, group_id, *, since=0, before=0, limit=_DEFAULT_LIMIT):
    legacy_messages = []
    try:
        orig_doc = db.collection(C.MESSAGES).document(group_id).get()
        if not orig_doc.exists:
            return legacy_messages
        for msg_id, msg_data in (orig_doc.to_dict() or {}).items():
            if not isinstance(msg_data, dict):
                continue
            ts = int(msg_data.get("timestamp", 0) or 0)
            if since and ts <= since:
                continue
            if before and ts >= before:
                continue
            legacy_messages.append(message_to_dict(msg_id, msg_data))
    except Exception:
        return []

    legacy_messages.sort(key=lambda m: m["timestamp"])
    if since:
        return legacy_messages[:limit]
    return legacy_messages[-limit:]


@messages_bp.route("", methods=["GET"])
@require_auth
def list_messages(group_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = _check_member(db, group_id, uid)
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    since = request.args.get("since", 0, type=int) or 0
    before = request.args.get("before", 0, type=int) or 0
    limit = _clamp_limit(request.args.get("limit", _DEFAULT_LIMIT, type=int))
    include_legacy = _is_true(request.args.get("includeLegacy")) and not since

    base_query = db.collection(C.MESSAGES).where("group_id", "==", group_id)
    reverse_after_fetch = False

    if since:
        query = base_query.where("timestamp", ">", since).order_by("timestamp").limit(limit)
    elif before:
        query = (
            base_query
            .where("timestamp", "<", before)
            .order_by("timestamp", direction=fs.Query.DESCENDING)
            .limit(limit)
        )
        reverse_after_fetch = True
    else:
        query = base_query.order_by("timestamp", direction=fs.Query.DESCENDING).limit(limit)
        reverse_after_fetch = True

    docs = list(query.get())
    if reverse_after_fetch:
        docs.reverse()

    msg_map = {d.id: message_to_dict(d.id, d.to_dict()) for d in docs}

    if include_legacy:
        for legacy_msg in _load_legacy_messages(
            db,
            group_id,
            since=since,
            before=before,
            limit=limit,
        ):
            msg_map.setdefault(legacy_msg["id"], legacy_msg)

    messages = sorted(msg_map.values(), key=lambda m: m["timestamp"])
    if not since and len(messages) > limit:
        messages = messages[-limit:]

    oldest_timestamp = messages[0]["timestamp"] if messages else None
    newest_timestamp = messages[-1]["timestamp"] if messages else None

    return jsonify({
        "messages": messages,
        "limit": limit,
        "oldestTimestamp": oldest_timestamp,
        "newestTimestamp": newest_timestamp,
        "hasMore": bool(oldest_timestamp) and len(messages) >= limit and not since,
    })


@messages_bp.route("", methods=["POST"])
@require_auth
def send_message(group_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, group_data = _check_member(db, group_id, uid)
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    data = request.get_json() or {}
    text = data.get("message", "").strip()
    if not text:
        return jsonify({"error": "message is required"}), 400

    user_doc = db.collection(C.USERS).document(uid).get()
    sender_name = user_doc.to_dict().get("name", "User") if user_doc.exists else "User"

    if group_data:
        is_group = (
            group_data.get("is_group", True)
            if "is_group" in group_data
            else group_data.get("isGroup", True)
        )
    else:
        is_group = True

    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    msg_id = str(uuid.uuid4())

    msg_data = {
        "group_id": group_id,
        "sender_id": uid,
        "sender_name": sender_name,
        "message": text,
        "is_group": is_group,
        "is_money_shared": False,
        "is_image_shared": False,
        "is_poll": False,
        "is_loan_request": False,
        "money": "",
        "image": "",
        "caption": "",
        "timestamp": now,
    }
    db.collection(C.MESSAGES).document(msg_id).set(msg_data)

    try:
        db.collection(C.MESSAGES).document(group_id).set({
            msg_id: {
                "id": msg_id,
                "senderID": uid,
                "senderName": sender_name,
                "receiverID": "",
                "receiverName": "",
                "chatID": group_id,
                "isGroup": is_group,
                "isMoneyShared": False,
                "isImageShared": False,
                "isPoll": False,
                "isLoanRequest": False,
                "money": "",
                "image": "",
                "caption": "",
                "message": text,
                "timestamp": now,
            }
        }, merge=True)
    except Exception:
        pass

    last_msg = f"{sender_name}: {text}" if is_group else text

    try:
        db.collection(C.GROUP_ACCOUNTS).document(group_id).update({
            "last_message": last_msg,
            "timestamp": now,
        })
    except Exception:
        pass

    chat_update = {
        f"{group_id}.timestamp": now,
        f"{group_id}.lastMessage": last_msg,
        f"{group_id}.isMoneyShared": False,
        f"{group_id}.isImageShared": False,
        f"{group_id}.isGroup": is_group,
        f"{group_id}.senderName": sender_name,
    }
    try:
        db.collection(C.CHATS).document(uid).set(chat_update, merge=True)

        gm_docs = (
            db.collection(C.GROUP_MEMBERS)
            .where("group_id", "==", group_id)
            .get()
        )
        for gm in gm_docs:
            member_uid = gm.to_dict().get("user_id", "")
            if member_uid and member_uid != uid:
                db.collection(C.CHATS).document(member_uid).set(
                    chat_update, merge=True
                )
    except Exception:
        pass

    try:
        if group_data and isinstance(group_data.get("groupMembers"), list):
            for member in group_data["groupMembers"]:
                if isinstance(member, dict):
                    member_uid = member.get("id") or member.get("uid", "")
                    if member_uid and member_uid != uid:
                        db.collection(C.CHATS).document(member_uid).set(
                            chat_update, merge=True
                        )
    except Exception:
        pass

    return jsonify({"message": message_to_dict(msg_id, msg_data)}), 201
