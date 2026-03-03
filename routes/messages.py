from flask import Blueprint, request, jsonify
from firebase_utils import get_db
from models import message_to_dict
from auth_utils import require_auth, get_jwt_identity
import db_constants as C
import uuid
from datetime import datetime, timezone

messages_bp = Blueprint("messages", __name__, url_prefix="/groups/<group_id>/messages")


def _check_member(db, group_id, uid):
    """Returns (is_member: bool, group_data: dict|None)."""
    # ── New backend: GroupAccounts + GroupMembers ──────────────────────────────
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
    # ── Original project: CHATS/{uid} map contains the group_id key ───────────
    try:
        chats_doc = db.collection(C.CHATS).document(uid).get()
        if chats_doc.exists:
            chat_data = chats_doc.to_dict() or {}
            if group_id in chat_data and isinstance(chat_data[group_id], dict):
                return True, chat_data[group_id]
    except Exception:
        pass
    return False, None


@messages_bp.route("", methods=["GET"])
@require_auth
def list_messages(group_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = _check_member(db, group_id, uid)
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    since = request.args.get("since", 0, type=int)

    # ── Source 1: new backend — flat MESSAGES collection ──────────────────────
    query = db.collection(C.MESSAGES).where("group_id", "==", group_id)
    if since:
        query = query.where("timestamp", ">", since)
    docs = query.get()
    msg_map = {d.id: message_to_dict(d.id, d.to_dict()) for d in docs}

    # ── Source 2: original project — MESSAGES/{chatId} map document ───────────
    try:
        orig_doc = db.collection(C.MESSAGES).document(group_id).get()
        if orig_doc.exists:
            for msg_id, msg_data in (orig_doc.to_dict() or {}).items():
                if isinstance(msg_data, dict) and msg_id not in msg_map:
                    ts = msg_data.get("timestamp", 0)
                    if not since or ts > since:
                        msg_map[msg_id] = message_to_dict(msg_id, msg_data)
    except Exception:
        pass

    messages = sorted(msg_map.values(), key=lambda m: m["timestamp"])
    return jsonify({"messages": messages})


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

    # ── Store in new backend flat MESSAGES collection ──────────────────────────
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

    # ── Also write to original MESSAGES/{chatId} map document format ───────────
    try:
        db.collection(C.MESSAGES).document(group_id).set({
            msg_id: {
                "id":            msg_id,
                "senderID":      uid,
                "senderName":    sender_name,
                "receiverID":    "",
                "receiverName":  "",
                "chatID":        group_id,
                "isGroup":       is_group,
                "isMoneyShared": False,
                "isImageShared": False,
                "isPoll":        False,
                "isLoanRequest": False,
                "money":         "",
                "image":         "",
                "caption":       "",
                "message":       text,
                "timestamp":     now,
            }
        }, merge=True)
    except Exception:
        pass

    last_msg = f"{sender_name}: {text}" if is_group else text

    # ── Update new backend GroupAccounts metadata ──────────────────────────────
    try:
        db.collection(C.GROUP_ACCOUNTS).document(group_id).update({
            "last_message": last_msg,
            "timestamp": now,
        })
    except Exception:
        pass

    # ── Update original CHATS/{uid} metadata for each group member ────────────
    chat_update = {
        f"{group_id}.timestamp":    now,
        f"{group_id}.lastMessage":  last_msg,
        f"{group_id}.isMoneyShared": False,
        f"{group_id}.isImageShared": False,
        f"{group_id}.isGroup":       is_group,
        f"{group_id}.senderName":    sender_name,
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

    # Also update members from original CHATS format (groupMembers embedded list)
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
