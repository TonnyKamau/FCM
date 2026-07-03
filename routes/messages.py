import logging
import requests

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
from config import API_KEY

messages_bp = Blueprint("messages", __name__, url_prefix="/groups/<group_id>/messages")

_DEFAULT_LIMIT = 100
_MAX_LIMIT = 200
_MEDIA_UPLOAD_URL = "https://api.kit-ifms.com/api/photos/upload"
_MAX_MEDIA_BYTES = 20 * 1024 * 1024
_ALLOWED_MEDIA_TYPES = {
    "audio/mp4", "audio/aac", "audio/mpeg", "audio/ogg", "audio/wav",
    "image/jpeg", "image/png", "image/webp",
}


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


def _message_collections(db, group_id):
    """All live message paths used by the backend and Android app."""
    return [
        db.collection(C.CHATS).document(group_id).collection(C.MESSAGES_SUBCOLLECTION),
        db.collection("MESSAGES").document(group_id).collection("messages"),
    ]


def _message_refs(db, group_id, message_id):
    return [collection.document(message_id) for collection in _message_collections(db, group_id)]


def _find_message(db, group_id, message_id):
    refs = _message_refs(db, group_id, message_id)
    for ref in refs:
        snapshot = ref.get()
        if snapshot.exists:
            return snapshot.to_dict() or {}, refs
    return None, refs


def _is_group_admin(group_data, uid):
    data = group_data or {}
    return uid in {
        data.get("admin_id", ""),
        data.get("adminID", ""),
        data.get("groupAdminId", ""),
    }


@messages_bp.route("/media", methods=["POST"])
@require_auth
def upload_message_media(group_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = _check_member(db, group_id, uid)
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    uploaded = request.files.get("file")
    if uploaded is None or not uploaded.filename:
        return jsonify({"error": "file is required"}), 400
    content_type = (uploaded.mimetype or "").lower()
    if content_type not in _ALLOWED_MEDIA_TYPES:
        return jsonify({"error": "Unsupported media type"}), 415

    payload = uploaded.read(_MAX_MEDIA_BYTES + 1)
    if not payload:
        return jsonify({"error": "File is empty"}), 400
    if len(payload) > _MAX_MEDIA_BYTES:
        return jsonify({"error": "File exceeds 20 MB"}), 413

    upload_type = "voice_note" if content_type.startswith("audio/") else "chat"
    try:
        upstream = requests.post(
            _MEDIA_UPLOAD_URL,
            headers={"X-API-KEY": API_KEY, "Accept": "application/json"},
            files={"image": (uploaded.filename, payload, content_type)},
            data={"upload_type": upload_type, "associated_id": group_id},
            timeout=180,
        )
        data = upstream.json()
    except Exception as exc:
        logging.exception("Media upload failed for %s: %s", group_id, exc)
        return jsonify({"error": "Media upload service unavailable"}), 502
    if not upstream.ok or data.get("success") is not True:
        return jsonify({"error": data.get("error", "Media upload failed")}), 502
    url = str(data.get("url", ""))
    if url and not url.startswith(("http://", "https://")):
        url = "https://api.kit-ifms.com/" + url.lstrip("/")
    return jsonify({"url": url, "mediaId": data.get("photo_id")}), 201


@messages_bp.route("/<message_id>/loan-action", methods=["POST"])
@require_auth
def loan_message_action(group_id, message_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, group_data = _check_member(db, group_id, uid)
    if not is_mem or not _is_group_admin(group_data, uid):
        return jsonify({"error": "Administrator access required"}), 403
    message, refs = _find_message(db, group_id, message_id)
    if not message or not message.get("isLoanApprovalRequest"):
        return jsonify({"error": "Loan approval card not found"}), 404
    action = (request.get_json() or {}).get("action", "").lower()
    if action not in {"approve", "reject"}:
        return jsonify({"error": "action must be approve or reject"}), 400
    request_id = message.get("loanApprovalRequestId")
    loan_ref = db.collection("LoanRequests").document(request_id)
    loan = loan_ref.get()
    if not loan.exists:
        return jsonify({"error": "Loan request not found"}), 404
    loan_data = loan.to_dict() or {}
    current = str(loan_data.get("adminApprovalStatus", "pending")).lower()
    desired = "approved" if action == "approve" else "rejected"
    if current not in {"", "pending", desired}:
        return jsonify({"error": f"Loan request is already {current}"}), 409
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    user_doc = db.collection(C.USERS).document(uid).get()
    admin_name = (user_doc.to_dict() or {}).get("name", "Admin") if user_doc.exists else "Admin"
    batch = db.batch()
    batch.set(loan_ref, {
        "adminApprovalStatus": desired,
        "adminId": uid,
        "adminName": admin_name,
        "adminApprovalTimestamp": now,
    }, merge=True)
    for ref in refs:
        batch.set(ref, {"actionStatus": desired, "actionedBy": uid, "actionedAt": now}, merge=True)
    if action == "approve" and current != "approved":
        approved_id = f"approved_{request_id}"
        approved = {
            "id": approved_id, "chatID": group_id,
            "senderID": loan_data.get("senderId", ""),
            "senderName": loan_data.get("senderName", ""),
            "message": f"{loan_data.get('senderName', 'Member')} requested a loan",
            "isGroup": True, "isLoanRequest": True,
            "loanRequestModel": loan_data, "timestamp": now,
        }
        for collection in _message_collections(db, group_id):
            batch.set(collection.document(approved_id), approved, merge=True)
    batch.commit()
    return jsonify({"status": desired})


@messages_bp.route("/<message_id>/guarantor-action", methods=["POST"])
@require_auth
def guarantor_message_action(group_id, message_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = _check_member(db, group_id, uid)
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403
    message, refs = _find_message(db, group_id, message_id)
    if not message or not message.get("isGuarantorInvitation"):
        return jsonify({"error": "Guarantor invitation not found"}), 404
    if message.get("requestedGuarantorId") != uid:
        return jsonify({"error": "Only the invited guarantor may respond"}), 403
    action = (request.get_json() or {}).get("action", "").lower()
    if action not in {"accept", "decline"}:
        return jsonify({"error": "action must be accept or decline"}), 400
    invitation_id = message.get("guarantorInvitationId")
    invitation_ref = db.collection("GuarantorInvitations").document(invitation_id)
    invitation = invitation_ref.get()
    if not invitation.exists:
        return jsonify({"error": "Invitation not found"}), 404
    invitation_data = invitation.to_dict() or {}
    current = str(invitation_data.get("status", "pending")).lower()
    desired = "accepted" if action == "accept" else "declined"
    if current not in {"pending", desired}:
        return jsonify({"error": f"Invitation is already {current}"}), 409
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    batch = db.batch()
    batch.set(invitation_ref, {"status": desired, "responseTimestamp": now}, merge=True)
    for ref in refs:
        batch.set(ref, {"actionStatus": desired, "actionedBy": uid, "actionedAt": now}, merge=True)
    if action == "accept" and current != "accepted":
        profile = db.collection(C.USERS).document(uid).get()
        user = profile.to_dict() or {}
        guarantor = {
            "userId": uid, "name": user.get("name", ""),
            "email": user.get("email", ""), "phoneNum": user.get("phoneNum", ""),
            "image": user.get("image", ""), "acceptedAt": now,
            "status": "active", "invitedBy": message.get("senderID", ""),
            "isKYCVerified": bool(user.get("isKYCVerified", False)),
        }
        if message.get("invitationType") == "group":
            target = (db.collection("Groups").document(group_id)
                      .collection("UserGuarantors").document(message.get("senderID", ""))
                      .collection("guarantors").document(uid))
        else:
            target = (db.collection("UserGuarantors").document(message.get("senderID", ""))
                      .collection("guarantors").document(uid))
        batch.set(target, guarantor, merge=True)
    batch.commit()
    return jsonify({"status": desired})




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

    msg_map = {}
    for base_query in _message_collections(db, group_id):
        try:
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

            docs = list(query.get())
            if reverse_after_fetch:
                docs.reverse()
            for doc in docs:
                try:
                    msg_map[doc.id] = message_to_dict(doc.id, doc.to_dict() or {})
                except Exception as exc:
                    logging.exception(
                        "Skipping malformed message doc %s in %s: %s",
                        doc.id,
                        group_id,
                        exc,
                    )
        except Exception as exc:
            logging.exception("Message query failed for %s: %s", group_id, exc)

    if include_legacy:
        for legacy_msg in _load_legacy_messages(
            db,
            group_id,
            since=since,
            before=before,
            limit=limit,
        ):
            msg_map.setdefault(legacy_msg["id"], legacy_msg)

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
        "lastMessage":   last_msg,
        "timestamp":     now,
        "isGroup":       is_group,
        "userID":        sender_uid,
        "isMoneyShared": False,
        "isImageShared": False,
        "isVoiceNote":   False,
        "whoShared":     "",
        "money":         "",
    }
    # Only write display fields when we actually have them — merging an empty
    # name/image over an existing preview turns the chat into "Unknown Chat".
    if gd.get("name"):
        chat_preview_base["name"] = gd["name"]
    if gd.get("image"):
        chat_preview_base["image"] = gd["image"]
    if admin_id:
        chat_preview_base["adminID"] = admin_id
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
        "replyToMessageId": "",
        "replyToSenderName": "",
        "replyToText": "",
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
        msg_data["replyToMessageId"] = data.get("replyToMessageId", "")
        msg_data["replyToSenderName"] = data.get("replyToSenderName", "")
        msg_data["replyToText"] = data.get("replyToText", "")
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
        # Android-compatible structure (PollModel.java): options are plain
        # strings; votes live in {votes: {index: count}, voters: {uid: index}}.
        poll_model = {
            "question": question,
            "options": [str(opt) for opt in raw_options],
            "senderId": uid,
            "votes": {"votes": {}, "voters": {}},
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
    batch = db.batch()
    for msg_ref in _message_refs(db, group_id, msg_id):
        batch.set(msg_ref, msg_data)
    batch.commit()

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
    existing_refs = []
    existing_data = None
    for candidate in _message_refs(db, group_id, message_id):
        snapshot = candidate.get()
        if snapshot.exists:
            existing_refs.append(candidate)
            if existing_data is None:
                existing_data = snapshot.to_dict() or {}
    if not existing_refs:
        return jsonify({"error": "Message not found"}), 404

    if action == "toggle":
        # Resolve toggle: add if uid not already in the list, remove otherwise
        existing_reactions = (existing_data or {}).get("reactions", {})
        action = "remove" if uid in existing_reactions.get(emoji, []) else "add"
    if action not in ("add", "remove"):
        return jsonify({"error": "action must be 'add', 'remove', or 'toggle'"}), 400

    try:
        field = f"reactions.{emoji}"
        for msg_ref in existing_refs:
            if action == "add":
                msg_ref.update({field: ArrayUnion([uid])})
            else:
                msg_ref.update({field: ArrayRemove([uid])})
    except Exception as exc:
        logging.exception("Failed to update reaction: %s", exc)
        return jsonify({"error": "Failed to update reaction"}), 500

    updated = existing_refs[0].get()
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

    existing_refs = []
    msg_doc = None
    for candidate in _message_refs(db, group_id, message_id):
        snapshot = candidate.get()
        if snapshot.exists:
            existing_refs.append(candidate)
            if msg_doc is None:
                msg_doc = snapshot
    if msg_doc is None:
        return jsonify({"error": "Message not found"}), 404

    msg_data = msg_doc.to_dict() or {}
    if not msg_data.get("isPoll"):
        return jsonify({"error": "Message is not a poll"}), 400

    poll_model = msg_data.get("pollModel") or {}
    options = poll_model.get("options", [])

    if options and isinstance(options[0], str):
        # ── Android structure: options are strings, optionId is the index ────
        try:
            idx = int(option_id)
        except ValueError:
            return jsonify({"error": "Option not found"}), 404
        if idx < 0 or idx >= len(options):
            return jsonify({"error": "Option not found"}), 404
        votes_obj = poll_model.get("votes") or {}
        counts = votes_obj.get("votes") or {}
        voters = votes_obj.get("voters") or {}
        if uid in voters:
            return jsonify({"error": "You have already voted on this poll"}), 400
        counts[str(idx)] = int(counts.get(str(idx), 0) or 0) + 1
        voters[uid] = idx
        poll_model["votes"] = {"votes": counts, "voters": voters}
    else:
        # ── Legacy structure: options are {id, text, votes} maps ────────────
        option_found = False
        for opt in options:
            voters = opt.get("voterIds", [])
            if uid in voters:
                return jsonify({"error": "You have already voted on this poll"}), 400
        for opt in options:
            if opt.get("id") == option_id:
                opt["votes"] = int(opt.get("votes", 0)) + 1
                opt.setdefault("voterIds", []).append(uid)
                option_found = True
        if not option_found:
            return jsonify({"error": "Option not found"}), 404
        poll_model["options"] = options
        poll_model["totalVotes"] = int(poll_model.get("totalVotes", 0)) + 1

    try:
        for msg_ref in existing_refs:
            msg_ref.update({"pollModel": poll_model})
    except Exception as exc:
        logging.exception("Failed to record vote: %s", exc)
        return jsonify({"error": "Failed to record vote"}), 500

    updated_msg = dict(msg_data)
    updated_msg["pollModel"] = poll_model
    return jsonify({"message": message_to_dict(message_id, updated_msg)})
