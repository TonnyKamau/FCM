"""
Direct Messages routes.

Paths
-----
GET  /messages/direct/<other_user_id>       — list DM history
POST /messages/direct/<other_user_id>       — send a DM
PUT  /messages/direct/<other_user_id>/read  — mark all messages as read
GET  /chats                                 — list ALL chats (groups + DMs) for the current user
"""

import logging
import uuid
from datetime import datetime, timezone

from flask import Blueprint, request, jsonify
from google.cloud import firestore as fs
from google.cloud.firestore import Increment

from firebase_utils import get_db
from models import message_to_dict
from auth_utils import require_auth, get_jwt_identity
import db_constants as C

direct_messages_bp = Blueprint("direct_messages", __name__)

_DEFAULT_LIMIT = 100
_MAX_LIMIT = 200


def _now_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _dm_chat_id(uid_a: str, uid_b: str) -> str:
    """Canonical chat ID: sorted UIDs joined with underscore."""
    return "_".join(sorted([uid_a, uid_b]))


def _clamp_limit(raw):
    try:
        limit = int(raw or _DEFAULT_LIMIT)
    except (TypeError, ValueError):
        limit = _DEFAULT_LIMIT
    return max(1, min(limit, _MAX_LIMIT))


# ── List DM history ────────────────────────────────────────────────────────────

@direct_messages_bp.route("/messages/direct/<other_user_id>", methods=["GET"])
@require_auth
def list_direct_messages(other_user_id):
    uid = get_jwt_identity()
    db = get_db()

    chat_id = _dm_chat_id(uid, other_user_id)
    since = request.args.get("since", 0, type=int) or 0
    before = request.args.get("before", 0, type=int) or 0
    limit = _clamp_limit(request.args.get("limit", _DEFAULT_LIMIT, type=int))

    base_query = (
        db.collection(C.CHATS)
        .document(chat_id)
        .collection(C.MESSAGES_SUBCOLLECTION)
    )

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
        query = (
            base_query
            .order_by("timestamp", direction=fs.Query.DESCENDING)
            .limit(limit)
        )
        reverse_after_fetch = True

    try:
        docs = list(query.get())
        if reverse_after_fetch:
            docs.reverse()
        messages = [message_to_dict(d.id, d.to_dict() or {}) for d in docs]
    except Exception as exc:
        logging.exception("Failed to list DMs for chat %s: %s", chat_id, exc)
        return jsonify({"error": "Failed to load messages"}), 500

    oldest_ts = messages[0]["timestamp"] if messages else None
    newest_ts = messages[-1]["timestamp"] if messages else None

    return jsonify({
        "messages": messages,
        "chatId": chat_id,
        "limit": limit,
        "oldestTimestamp": oldest_ts,
        "newestTimestamp": newest_ts,
        "hasMore": bool(oldest_ts) and len(messages) >= limit and not since,
    })


# ── Send DM ────────────────────────────────────────────────────────────────────

@direct_messages_bp.route("/messages/direct/<other_user_id>", methods=["POST"])
@require_auth
def send_direct_message(other_user_id):
    uid = get_jwt_identity()
    db = get_db()

    # Fetch both users
    sender_doc = db.collection(C.USERS).document(uid).get()
    sender_data = sender_doc.to_dict() if sender_doc.exists else {}
    sender_name = sender_data.get("name", "User")

    receiver_doc = db.collection(C.USERS).document(other_user_id).get()
    if not receiver_doc.exists:
        return jsonify({"error": "Recipient user not found"}), 404
    receiver_data = receiver_doc.to_dict() or {}
    receiver_name = receiver_data.get("name", "User")

    data = request.get_json() or {}
    msg_type = data.get("type", "text").lower()

    chat_id = _dm_chat_id(uid, other_user_id)
    now = _now_ms()
    msg_id = str(uuid.uuid4())

    # Base message
    msg_data = {
        "id":            msg_id,
        "senderID":      uid,
        "senderName":    sender_name,
        "receiverID":    other_user_id,
        "receiverName":  receiver_name,
        "chatID":        chat_id,
        "message":       "",
        "isGroup":       False,
        "isMoneyShared": False,
        "isImageShared": False,
        "isPoll":        False,
        "isVoiceNote":   False,
        "isLoanRequest": False,
        "money":         "",
        "image":         "",
        "caption":       "",
        "reactions":     {},
        "timestamp":     now,
    }
    extra_preview = {}

    if msg_type == "text":
        text = data.get("message", "").strip()
        if not text:
            return jsonify({"error": "message is required"}), 400
        msg_data["message"] = text
        last_msg = text

    elif msg_type == "money":
        # Flutter sends 'money' (a string); also accept numeric 'amount'
        amount = data.get("amount") if data.get("amount") is not None else data.get("money")
        if amount is None:
            return jsonify({"error": "amount (or money) is required for money messages"}), 400
        try:
            amount = float(amount)
        except (TypeError, ValueError):
            return jsonify({"error": "amount must be a number"}), 400
        money_str = f"KES {amount:.2f}"
        msg_data["isMoneyShared"] = True
        msg_data["money"] = money_str
        msg_data["whoShared"] = sender_name
        msg_data["message"] = f"{sender_name} shared {money_str}"
        last_msg = msg_data["message"]
        extra_preview = {"isMoneyShared": True, "money": money_str, "whoShared": sender_name}

    elif msg_type == "image":
        image_data = data.get("image", "")
        if not image_data:
            return jsonify({"error": "image is required for image messages"}), 400
        caption = data.get("caption", "").strip()
        msg_data["isImageShared"] = True
        msg_data["image"] = image_data
        msg_data["caption"] = caption
        msg_data["message"] = caption or f"{sender_name} shared an image"
        last_msg = "\U0001f4f7 Photo"
        extra_preview = {"isImageShared": True}

    else:
        return jsonify({"error": f"Unsupported message type for DMs: {msg_type}"}), 400

    # Write message
    try:
        (
            db.collection(C.CHATS)
            .document(chat_id)
            .collection(C.MESSAGES_SUBCOLLECTION)
            .document(msg_id)
            .set(msg_data)
        )
    except Exception as exc:
        logging.exception("Failed to write DM: %s", exc)
        return jsonify({"error": "Failed to send message"}), 500

    # Build preview base
    sender_image = sender_data.get("image_url", "") or sender_data.get("image", "")
    receiver_image = receiver_data.get("image_url", "") or receiver_data.get("image", "")

    sender_preview = {
        "id":            chat_id,
        "name":          receiver_name,
        "image":         receiver_image,
        "lastMessage":   last_msg,
        "timestamp":     now,
        "isGroup":       False,
        "adminID":       "",
        "userID":        uid,
        "otherUserId":   other_user_id,
        "isMoneyShared": False,
        "isImageShared": False,
        "isVoiceNote":   False,
        "whoShared":     "",
        "money":         "",
        "unreadCount":   0,
    }
    receiver_preview = {
        "id":            chat_id,
        "name":          sender_name,
        "image":         sender_image,
        "lastMessage":   last_msg,
        "timestamp":     now,
        "isGroup":       False,
        "adminID":       "",
        "userID":        other_user_id,
        "otherUserId":   uid,
        "isMoneyShared": False,
        "isImageShared": False,
        "isVoiceNote":   False,
        "whoShared":     "",
        "money":         "",
        "unreadCount":   Increment(1),
    }
    sender_preview.update(extra_preview)
    receiver_preview.update(extra_preview)

    # Never merge empty display fields over an existing preview — an empty
    # name/image would turn the chat into "Unknown Chat" in the apps.
    for preview in (sender_preview, receiver_preview):
        for field in ("name", "image"):
            if not preview.get(field):
                preview.pop(field, None)

    try:
        (
            db.collection(C.USER_CHAT_PREVIEWS)
            .document(uid)
            .collection(C.CHATS_SUBCOLLECTION)
            .document(chat_id)
            .set(sender_preview, merge=True)
        )
        (
            db.collection(C.USER_CHAT_PREVIEWS)
            .document(other_user_id)
            .collection(C.CHATS_SUBCOLLECTION)
            .document(chat_id)
            .set(receiver_preview, merge=True)
        )
    except Exception as exc:
        logging.warning("Failed to write DM chat previews: %s", exc)

    return jsonify({"message": message_to_dict(msg_id, msg_data)}), 201


# ── Mark as read ───────────────────────────────────────────────────────────────

@direct_messages_bp.route("/messages/direct/<other_user_id>/read", methods=["PUT"])
@require_auth
def mark_dm_read(other_user_id):
    uid = get_jwt_identity()
    db = get_db()

    chat_id = _dm_chat_id(uid, other_user_id)
    try:
        (
            db.collection(C.USER_CHAT_PREVIEWS)
            .document(uid)
            .collection(C.CHATS_SUBCOLLECTION)
            .document(chat_id)
            .set({"unreadCount": 0}, merge=True)
        )
    except Exception as exc:
        logging.exception("Failed to mark DM as read: %s", exc)
        return jsonify({"error": "Failed to mark as read"}), 500

    return jsonify({"success": True, "chatId": chat_id})


# ── List all chats ─────────────────────────────────────────────────────────────

@direct_messages_bp.route("/chats", methods=["GET"])
@require_auth
def list_all_chats():
    uid = get_jwt_identity()
    db = get_db()

    try:
        preview_docs = (
            db.collection(C.USER_CHAT_PREVIEWS)
            .document(uid)
            .collection(C.CHATS_SUBCOLLECTION)
            .order_by("timestamp", direction=fs.Query.DESCENDING)
            .limit(200)
            .get()
        )
        chats = []
        for doc in preview_docs:
            d = doc.to_dict() or {}
            chats.append({
                "id":            doc.id,
                "name":          d.get("name", ""),
                "image":         d.get("image", ""),
                "lastMessage":   d.get("lastMessage", ""),
                "timestamp":     d.get("timestamp", 0),
                "isGroup":       d.get("isGroup", False),
                "adminID":       d.get("adminID", ""),
                "userID":        d.get("userID", uid),
                "otherUserId":   d.get("otherUserId", ""),
                "unreadCount":   int(d.get("unreadCount", 0) or 0),
                "isMoneyShared": d.get("isMoneyShared", False),
                "isImageShared": d.get("isImageShared", False),
                "isVoiceNote":   d.get("isVoiceNote", False),
                "whoShared":     d.get("whoShared", ""),
                "money":         d.get("money", ""),
                "isBusinessGroup": d.get("isBusinessGroup", False),
            })
        return jsonify({"chats": chats, "count": len(chats)})
    except Exception as exc:
        logging.exception("Failed to list chats for user %s: %s", uid, exc)
        return jsonify({"error": "Failed to load chats"}), 500
