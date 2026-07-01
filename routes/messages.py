import logging

from flask import Blueprint, request, jsonify
from google.cloud import firestore as fs
from google.cloud.firestore_v1 import ArrayUnion, ArrayRemove

from firebase_utils import get_db
from models import message_to_dict
from auth_utils import require_auth, get_jwt_identity
from google.cloud.firestore import Increment
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
    # ── New subcollection structure: USER_CHAT_PREVIEWS/{uid}/CHATS/{group_id} ──
    try:
        preview_doc = (
            db.collection(C.USER_CHAT_PREVIEWS)
            .document(uid)
            .collection(C.CHATS_SUBCOLLECTION)
            .document(group_id)
            .get()
        )
        if preview_doc.exists:
            return True, preview_doc.to_dict()
    except Exception:
        pass
    # ── Legacy: CHATS/{uid} map contains the group_id key ─────────────────────
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
        # Legacy path: MESSAGES/{groupId} map doc
        orig_doc = db.collection("MESSAGES").document(group_id).get()
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
    canonical_only = _is_true(request.args.get("canonical"))
    include_legacy = _is_true(request.args.get("includeLegacy")) and not since and not canonical_only

    base_query = db.collection(C.CHATS).document(group_id).collection(C.MESSAGES_SUBCOLLECTION)
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

    msg_map = {}
    try:
        docs = list(query.get())
        if reverse_after_fetch:
            docs.reverse()

        for doc in docs:
            data = doc.to_dict() or {}
            try:
                msg_map[doc.id] = message_to_dict(doc.id, data)
            except Exception as exc:
                logging.exception("Skipping malformed message doc %s in %s: %s", doc.id, group_id, exc)
    except Exception as exc:
        logging.exception("Flat message query failed for %s: %s", group_id, exc)

    if include_legacy and not msg_map:
        try:
            for legacy_msg in _load_legacy_messages(
                db,
                group_id,
                since=since,
                before=before,
                limit=limit,
            ):
                msg_map.setdefault(legacy_msg["id"], legacy_msg)
        except Exception as exc:
            logging.exception("Legacy message load failed for %s: %s", group_id, exc)

    messages = sorted(msg_map.values(), key=lambda m: int(m.get("timestamp", 0) or 0))
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


# ─── helpers ────────────────────────────────────────────────────────────────

def _get_group_members(db, group_id, group_data):
    """Return a set of all member user-ids for a group."""
    all_member_ids = set()
    try:
        gm_docs = db.collection(C.GROUP_MEMBERS).where("group_id", "==", group_id).get()
        for gm in gm_docs:
            mid = gm.to_dict().get("user_id", "")
            if mid:
                all_member_ids.add(mid)
    except Exception:
        pass
    try:
        if group_data and isinstance(group_data.get("groupMembers"), list):
            for member in group_data["groupMembers"]:
                if isinstance(member, dict):
                    mid = member.get("id") or member.get("uid", "")
                    if mid:
                        all_member_ids.add(mid)
    except Exception:
        pass
    return all_member_ids


def _update_chat_previews(db, sender_uid, group_id, group_data, last_msg, now, extra_preview_fields=None):
    """Write USER_CHAT_PREVIEWS/{memberId}/CHATS/{groupId} for all members."""
    all_member_ids = _get_group_members(db, group_id, group_data)
    all_member_ids.add(sender_uid)

    gd = group_data or {}
    is_group = gd.get("is_group", True) if "is_group" in gd else gd.get("isGroup", True)
    admin_id = gd.get("admin_id", "") or gd.get("adminID", "")

    chat_preview_base = {
        "id":            group_id,
        "name":          gd.get("name", ""),
        "image":         gd.get("image", ""),
        "lastMessage":   last_msg,
        "timestamp":     now,
        "isGroup":       is_group,
        "adminID":       admin_id,
        "userID":        sender_uid,
        "isMoneyShared": False,
        "isImageShared": False,
        "isVoiceNote":   False,
        "whoShared":     "",
        "money":         "",
    }
    if extra_preview_fields:
        chat_preview_base.update(extra_preview_fields)

    try:
        for member_uid in all_member_ids:
            preview_ref = (
                db.collection(C.USER_CHAT_PREVIEWS)
                .document(member_uid)
                .collection(C.CHATS_SUBCOLLECTION)
                .document(group_id)
            )
            preview_data = dict(chat_preview_base)
            preview_data["unreadCount"] = 0 if member_uid == sender_uid else Increment(1)
            preview_ref.set(preview_data, merge=True)
    except Exception:
        pass


def _is_group_flag(group_data):
    if group_data:
        return group_data.get("is_group", True) if "is_group" in group_data else group_data.get("isGroup", True)
    return True


def _base_msg(msg_id, uid, sender_name, group_id, is_group, now):
    return {
        "id":            msg_id,
        "senderID":      uid,
        "senderName":    sender_name,
        "receiverID":    "",
        "receiverName":  "",
        "chatID":        group_id,
        "message":       "",
        "isGroup":       is_group,
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


# ─── send message ────────────────────────────────────────────────────────────

@messages_bp.route("", methods=["POST"])
@require_auth
def send_message(group_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, group_data = _check_member(db, group_id, uid)
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    data = request.get_json() or {}
    msg_type = data.get("type", "text").lower()

    user_doc = db.collection(C.USERS).document(uid).get()
    sender_name = user_doc.to_dict().get("name", "User") if user_doc.exists else "User"
    is_group = _is_group_flag(group_data)
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    msg_id = str(uuid.uuid4())

    msg_data = _base_msg(msg_id, uid, sender_name, group_id, is_group, now)
    extra_preview = {}

    if msg_type == "text":
        text = data.get("message", "").strip()
        if not text:
            return jsonify({"error": "message is required"}), 400
        msg_data["message"] = text
        last_msg = f"{sender_name}: {text}" if is_group else text

    elif msg_type == "money":
        # Flutter sends 'money' (a string like "100"); also accept numeric 'amount'
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
        last_msg = f"{sender_name}: 📷 Photo" if is_group else "📷 Photo"
        extra_preview = {"isImageShared": True}

    elif msg_type == "voice_note":
        voice_url = data.get("voiceNoteUrl", "")
        if not voice_url:
            return jsonify({"error": "voiceNoteUrl is required for voice_note messages"}), 400
        duration = data.get("voiceNoteDuration", 0)
        try:
            duration = int(duration)
        except (TypeError, ValueError):
            duration = 0
        msg_data["isVoiceNote"] = True
        msg_data["voiceNoteUrl"] = voice_url
        msg_data["voiceNoteDuration"] = duration
        msg_data["message"] = f"{sender_name} sent a voice note"
        last_msg = f"{sender_name}: 🎤 Voice note" if is_group else "🎤 Voice note"
        extra_preview = {"isVoiceNote": True}

    elif msg_type == "poll":
        # Flutter sends either top-level question+options OR a nested pollModel object
        nested_poll = data.get("pollModel") or {}
        question = (data.get("question") or nested_poll.get("question") or "").strip()
        raw_options = data.get("options") or [
            o.get("text", "") for o in (nested_poll.get("options") or [])
        ]
        if not question:
            return jsonify({"error": "question is required for poll messages"}), 400
        if not raw_options or not isinstance(raw_options, list) or len(raw_options) < 2:
            return jsonify({"error": "at least 2 options are required for poll messages"}), 400
        poll_options = [
            {"id": str(uuid.uuid4()), "text": str(opt), "votes": 0}
            for opt in raw_options
        ]
        poll_model = {
            "question": question,
            "options": poll_options,
            "senderId": uid,
            "totalVotes": 0,
        }
        msg_data["isPoll"] = True
        msg_data["pollModel"] = poll_model
        msg_data["message"] = f"{sender_name} created a poll: {question}"
        last_msg = f"{sender_name}: 📊 Poll"

    elif msg_type == "reaction":
        # Handled by its own endpoint; reject if sent here
        return jsonify({"error": "Use PUT /groups/<group_id>/messages/<message_id>/react for reactions"}), 400

    else:
        return jsonify({"error": f"Unknown message type: {msg_type}"}), 400

    # Write message
    db.collection(C.CHATS).document(group_id).collection(C.MESSAGES_SUBCOLLECTION).document(msg_id).set(msg_data)

    # Update group document last_message
    try:
        db.collection(C.GROUP_ACCOUNTS).document(group_id).update({
            "last_message": last_msg,
            "timestamp": now,
        })
    except Exception:
        pass

    # Update previews for all members
    _update_chat_previews(db, uid, group_id, group_data, last_msg, now, extra_preview)

    return jsonify({"message": message_to_dict(msg_id, msg_data)}), 201


# ─── react to message ────────────────────────────────────────────────────────

@messages_bp.route("/<message_id>/react", methods=["PUT"])
@require_auth
def react_to_message(group_id, message_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = _check_member(db, group_id, uid)
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    data = request.get_json() or {}
    emoji = data.get("emoji", "").strip()
    # Flutter sends action='toggle'; treat it as add-or-remove based on current state
    action = data.get("action", "add").lower()
    if not emoji:
        return jsonify({"error": "emoji is required"}), 400
    if action == "toggle":
        # Resolve toggle: add if uid not already in the list, remove otherwise
        msg_doc_check = (
            db.collection(C.CHATS)
            .document(group_id)
            .collection(C.MESSAGES_SUBCOLLECTION)
            .document(message_id)
            .get()
        )
        if not msg_doc_check.exists:
            return jsonify({"error": "Message not found"}), 404
        existing_reactions = (msg_doc_check.to_dict() or {}).get("reactions", {})
        action = "remove" if uid in existing_reactions.get(emoji, []) else "add"
    if action not in ("add", "remove"):
        return jsonify({"error": "action must be 'add', 'remove', or 'toggle'"}), 400

    msg_ref = (
        db.collection(C.CHATS)
        .document(group_id)
        .collection(C.MESSAGES_SUBCOLLECTION)
        .document(message_id)
    )
    if not msg_ref.get().exists:
        return jsonify({"error": "Message not found"}), 404

    try:
        field = f"reactions.{emoji}"
        if action == "add":
            msg_ref.update({field: ArrayUnion([uid])})
        else:
            msg_ref.update({field: ArrayRemove([uid])})
    except Exception as exc:
        logging.exception("Failed to update reaction: %s", exc)
        return jsonify({"error": "Failed to update reaction"}), 500

    updated = msg_ref.get()
    return jsonify({"message": message_to_dict(message_id, updated.to_dict() or {})})


# ─── poll vote ───────────────────────────────────────────────────────────────

@messages_bp.route("/<message_id>/poll/vote", methods=["POST"])
@require_auth
def vote_on_poll(group_id, message_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = _check_member(db, group_id, uid)
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    data = request.get_json() or {}
    option_id = data.get("optionId", "").strip()
    if not option_id:
        return jsonify({"error": "optionId is required"}), 400

    msg_ref = (
        db.collection(C.CHATS)
        .document(group_id)
        .collection(C.MESSAGES_SUBCOLLECTION)
        .document(message_id)
    )
    msg_doc = msg_ref.get()
    if not msg_doc.exists:
        return jsonify({"error": "Message not found"}), 404

    msg_data = msg_doc.to_dict() or {}
    if not msg_data.get("isPoll"):
        return jsonify({"error": "Message is not a poll"}), 400

    poll_model = msg_data.get("pollModel") or {}
    options = poll_model.get("options", [])

    # Find the option
    option_found = False
    for opt in options:
        if opt.get("id") == option_id:
            opt["votes"] = int(opt.get("votes", 0)) + 1
            option_found = True
        # Track voter ids per option to prevent double votes
        voters = opt.get("voterIds", [])
        if uid in voters:
            return jsonify({"error": "You have already voted on this poll"}), 400

    if not option_found:
        return jsonify({"error": "Option not found"}), 404

    # Mark uid as voter
    for opt in options:
        if opt.get("id") == option_id:
            voters = opt.get("voterIds", [])
            voters.append(uid)
            opt["voterIds"] = voters

    poll_model["options"] = options
    poll_model["totalVotes"] = int(poll_model.get("totalVotes", 0)) + 1

    try:
        msg_ref.update({"pollModel": poll_model})
    except Exception as exc:
        logging.exception("Failed to record vote: %s", exc)
        return jsonify({"error": "Failed to record vote"}), 500

    updated_msg = dict(msg_data)
    updated_msg["pollModel"] = poll_model
    return jsonify({"message": message_to_dict(message_id, updated_msg)})
