import logging

from flask import Blueprint, request, jsonify
from firebase_admin import messaging
from firebase_utils import get_db
from models import group_to_dict, group_member_to_dict
from auth_utils import require_auth, get_jwt_identity
from cache_utils import get_cached_user_payload, set_cached_user_payload, invalidate_user_payload
import db_constants as C
import uuid
from datetime import datetime, timezone

groups_bp = Blueprint("groups", __name__, url_prefix="/groups")


def _now_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _is_true(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _has_usable_display(group_dict):
    return bool(str(group_dict.get("id", "")).strip()) and bool(
        str(group_dict.get("name", "")).strip()
    )


def _member_summary(user_id, role, user_data=None, fallback_email=""):
    data = user_data or {}
    image = data.get("image_url", "") or data.get("image", "")
    return {
        "user_id": user_id,
        "role": role,
        "member_name": data.get("name", "") or "",
        "member_email": data.get("email", "") or fallback_email,
        "member_phone": data.get("phone", "") or data.get("phoneNum", "") or "",
        "member_image": image,
        "member_photo_url": image,
    }


def _build_group(db, group_id):
    """Fetch a group document and its members; returns group dict or None."""
    doc = db.collection(C.GROUP_ACCOUNTS).document(group_id).get()
    if not doc.exists:
        return None
    gd = doc.to_dict()

    member_docs = db.collection(C.GROUP_MEMBERS).where("group_id", "==", group_id).get()
    members = [group_member_to_dict(md.to_dict()) for md in member_docs]

    return group_to_dict(doc.id, gd, members)


@groups_bp.route("", methods=["GET"])
@require_auth
def list_groups():
    uid = get_jwt_identity()
    db = get_db()
    canonical_only = _is_true(request.args.get("canonical"))

    cache_name = "groups_canonical" if canonical_only else "groups"
    cached_payload = get_cached_user_payload(cache_name, uid)
    if cached_payload is not None:
        return jsonify(cached_payload)

    result = []
    seen_ids = set()

    member_docs = db.collection(C.GROUP_MEMBERS).where("user_id", "==", uid).get()
    member_by_group = {}
    group_ids = set()
    for member_doc in member_docs:
        member_data = member_doc.to_dict() or {}
        group_id = member_data.get("group_id")
        if not group_id:
            continue
        group_ids.add(group_id)
        member_by_group[group_id] = group_member_to_dict(member_data)

    owned_docs = db.collection(C.GROUP_ACCOUNTS).where("admin_id", "==", uid).get()
    for doc in owned_docs:
        result.append(group_to_dict(doc.id, doc.to_dict() or {}, members=[]))
        seen_ids.add(doc.id)
        group_ids.add(doc.id)

    for gid in group_ids:
        if gid in seen_ids:
            continue
        group_doc = db.collection(C.GROUP_ACCOUNTS).document(gid).get()
        if not group_doc.exists:
            continue
        member = member_by_group.get(gid)
        members = [member] if member else []
        result.append(group_to_dict(group_doc.id, group_doc.to_dict() or {}, members))
        seen_ids.add(gid)

    if not canonical_only:
        # ── New structure: USER_CHAT_PREVIEWS/{uid}/CHATS subcollection ──────
        try:
            preview_docs = (
                db.collection(C.USER_CHAT_PREVIEWS)
                .document(uid)
                .collection(C.CHATS_SUBCOLLECTION)
                .get()
            )
            for pdoc in preview_docs:
                group_data = pdoc.to_dict() or {}
                if not group_data.get("isBusinessGroup", False):
                    continue
                group_id_key = pdoc.id
                if group_id_key in seen_ids:
                    continue
                actual_id = group_data.get("id", group_id_key)
                result.append(group_to_dict(actual_id, group_data))
                seen_ids.add(actual_id)
        except Exception:
            pass
        # ── Legacy: CHATS/{uid} map ───────────────────────────────────────────
        try:
            chats_doc = db.collection(C.CHATS).document(uid).get()
            if chats_doc.exists:
                chat_data = chats_doc.to_dict() or {}
                for group_id, group_data in chat_data.items():
                    if not isinstance(group_data, dict):
                        continue
                    if not group_data.get("isBusinessGroup", False):
                        continue
                    if group_id in seen_ids:
                        continue

                    actual_id = group_data.get("id", group_id)
                    result.append(group_to_dict(actual_id, group_data))
                    seen_ids.add(actual_id)
        except Exception:
            pass

    result = [group for group in result if _has_usable_display(group)]
    result.sort(key=lambda g: g.get("timestamp", 0), reverse=True)
    payload = {"groups": result}
    set_cached_user_payload(cache_name, uid, payload)
    return jsonify(payload)


@groups_bp.route("", methods=["POST"])
@require_auth
def create_group():
    uid = get_jwt_identity()
    data = request.get_json() or {}
    name = data.get("name", "").strip()
    if not name:
        return jsonify({"error": "name is required"}), 400

    db = get_db()
    group_id = str(uuid.uuid4())
    now = _now_ms()

    group_data = {
        "name": name,
        "image": data.get("image", ""),
        "admin_id": uid,
        "is_business_group": data.get("isBusinessGroup", True),
        "is_group": data.get("isGroup", True),
        "is_money_shared": data.get("isMoneyShared", False),
        "restrict_money_after_loan": data.get("restrictMoneyAfterLoanRequest", False),
        "require_admin_approval_loans": data.get("requireAdminApprovalForLoans", False),
        "last_message": "",
        "timestamp": now,
        "created_at": now,
    }
    db.collection(C.GROUP_ACCOUNTS).document(group_id).set(group_data)

    creator_doc = db.collection(C.USERS).document(uid).get()
    creator_data = creator_doc.to_dict() if creator_doc.exists else {}

    owner_member = {
        "group_id": group_id,
        **_member_summary(uid, "OWNER", creator_data),
    }
    db.collection(C.GROUP_MEMBERS).document(str(uuid.uuid4())).set(owner_member)
    invalidate_user_payload("groups", uid)
    invalidate_user_payload("groups_canonical", uid)

    for m in data.get("members", []):
        email = m.get("email", "").strip().lower()
        if email:
            user_docs = list(
                db.collection(C.USERS).where("email", "==", email).limit(1).get()
            )
            if user_docs and user_docs[0].id != uid:
                user_data = user_docs[0].to_dict() or {}
                member_payload = {
                    "group_id": group_id,
                    **_member_summary(
                        user_docs[0].id,
                        m.get("role", "member"),
                        user_data,
                        fallback_email=email,
                    ),
                }
                db.collection(C.GROUP_MEMBERS).document(str(uuid.uuid4())).set(member_payload)
                invalidate_user_payload("groups", user_docs[0].id)
                invalidate_user_payload("groups_canonical", user_docs[0].id)

    return jsonify({"group": _build_group(db, group_id)}), 201


@groups_bp.route("/<group_id>", methods=["GET"])
@require_auth
def get_group(group_id):
    db = get_db()
    g_dict = _build_group(db, group_id)
    if not g_dict:
        return jsonify({"error": "Group not found"}), 404
    return jsonify({"group": g_dict})


@groups_bp.route("/<group_id>/members/<member_id>/role", methods=["PUT"])
@require_auth
def assign_role(group_id, member_id):
    uid = get_jwt_identity()
    db = get_db()

    group_doc = db.collection(C.GROUP_ACCOUNTS).document(group_id).get()
    if not group_doc.exists:
        return jsonify({"error": "Group not found"}), 404

    gd = group_doc.to_dict()
    if gd.get("admin_id") != uid:
        return jsonify({"error": "Only the group owner can assign roles"}), 403
    if member_id == uid:
        return jsonify({"error": "Cannot change owner role"}), 400

    data = request.get_json() or {}
    new_role = data.get("role", "member")

    gm_docs = list(
        db.collection(C.GROUP_MEMBERS)
        .where("group_id", "==", group_id)
        .where("user_id", "==", member_id)
        .limit(1).get()
    )
    if not gm_docs:
        return jsonify({"error": "Member not found in group"}), 404

    gm_docs[0].reference.update({"role": new_role})
    invalidate_user_payload("groups", uid)
    invalidate_user_payload("groups_canonical", uid)
    invalidate_user_payload("groups", member_id)
    invalidate_user_payload("groups_canonical", member_id)
    return jsonify({"group": _build_group(db, group_id)})


# ─── Member management ──────────────────────────────────────────────────────────

@groups_bp.route("/<group_id>/members", methods=["GET"])
@require_auth
def list_members(group_id):
    uid = get_jwt_identity()
    db = get_db()
    group_doc = db.collection(C.GROUP_ACCOUNTS).document(group_id).get()
    if not group_doc.exists:
        return jsonify({"error": "Group not found"}), 404

    member_docs = db.collection(C.GROUP_MEMBERS).where("group_id", "==", group_id).get()
    members = []
    for md in member_docs:
        mdata = md.to_dict() or {}
        members.append({
            "id":       mdata.get("user_id", ""),
            "role":     mdata.get("role", "member"),
            "name":     mdata.get("member_name", ""),
            "email":    mdata.get("member_email", ""),
            "phone":    mdata.get("member_phone", ""),
            "image":    mdata.get("member_image", ""),
            "photoUrl": mdata.get("member_photo_url", ""),
        })
    return jsonify({"members": members})


@groups_bp.route("/<group_id>/members", methods=["POST"])
@require_auth
def add_member(group_id):
    uid = get_jwt_identity()
    db = get_db()
    group_doc = db.collection(C.GROUP_ACCOUNTS).document(group_id).get()
    if not group_doc.exists:
        return jsonify({"error": "Group not found"}), 404
    gd = group_doc.to_dict() or {}

    # Only admin / owner can add members
    if gd.get("admin_id") != uid:
        # Allow if caller is an OWNER-role member
        caller_docs = list(
            db.collection(C.GROUP_MEMBERS)
            .where("group_id", "==", group_id)
            .where("user_id", "==", uid)
            .limit(1).get()
        )
        if not caller_docs or caller_docs[0].to_dict().get("role", "").upper() not in ("OWNER", "ADMIN"):
            return jsonify({"error": "Only group admins can add members"}), 403

    data = request.get_json() or {}
    email = data.get("email", "").strip().lower()
    user_id = data.get("userId", "").strip()
    role = data.get("role", "member")

    if not email and not user_id:
        return jsonify({"error": "email or userId is required"}), 400

    # Look up user
    new_user_doc = None
    new_uid = user_id
    if email:
        user_docs = list(db.collection(C.USERS).where("email", "==", email).limit(1).get())
        if not user_docs:
            return jsonify({"error": f"No user found with email {email}"}), 404
        new_user_doc = user_docs[0]
        new_uid = new_user_doc.id
    elif user_id:
        doc = db.collection(C.USERS).document(user_id).get()
        if not doc.exists:
            return jsonify({"error": "User not found"}), 404
        new_user_doc = doc

    if new_uid == uid and gd.get("admin_id") == uid:
        return jsonify({"error": "Admin is already a member"}), 400

    # Check if already a member
    existing = list(
        db.collection(C.GROUP_MEMBERS)
        .where("group_id", "==", group_id)
        .where("user_id", "==", new_uid)
        .limit(1).get()
    )
    if existing:
        return jsonify({"error": "User is already a member of this group"}), 409

    user_data = new_user_doc.to_dict() if new_user_doc else {}
    member_payload = {
        "group_id": group_id,
        **_member_summary(new_uid, role, user_data, fallback_email=email),
    }
    db.collection(C.GROUP_MEMBERS).document(str(uuid.uuid4())).set(member_payload)

    # Write chat preview for new member
    now = _now_ms()
    preview_ref = (
        db.collection(C.USER_CHAT_PREVIEWS)
        .document(new_uid)
        .collection(C.CHATS_SUBCOLLECTION)
        .document(group_id)
    )
    preview_data = {
        "id":            group_id,
        "name":          gd.get("name", ""),
        "image":         gd.get("image", ""),
        "lastMessage":   gd.get("last_message", ""),
        "timestamp":     now,
        "isGroup":       gd.get("is_group", True),
        "adminID":       gd.get("admin_id", ""),
        "userID":        new_uid,
        "unreadCount":   0,
        "isMoneyShared": False,
        "isImageShared": False,
        "isVoiceNote":   False,
        "whoShared":     "",
        "money":         "",
        "isBusinessGroup": gd.get("is_business_group", True),
    }
    try:
        preview_ref.set(preview_data, merge=True)
    except Exception as exc:
        logging.warning("Could not write chat preview for new member: %s", exc)

    # Send FCM notification
    try:
        fcm_token = user_data.get(C.FCM_TOKEN_FIELD, "")
        if fcm_token:
            group_name = gd.get("name", "a group")
            fcm_message = messaging.Message(
                notification=messaging.Notification(
                    title="Added to group",
                    body=f"You have been added to {group_name}",
                ),
                token=fcm_token,
            )
            messaging.send(fcm_message)
    except Exception as exc:
        logging.warning("FCM notification failed for new member: %s", exc)

    invalidate_user_payload("groups", new_uid)
    invalidate_user_payload("groups_canonical", new_uid)

    return jsonify({"group": _build_group(db, group_id)}), 201


@groups_bp.route("/<group_id>/members/<member_id>", methods=["DELETE"])
@require_auth
def remove_member(group_id, member_id):
    uid = get_jwt_identity()
    db = get_db()
    group_doc = db.collection(C.GROUP_ACCOUNTS).document(group_id).get()
    if not group_doc.exists:
        return jsonify({"error": "Group not found"}), 404
    gd = group_doc.to_dict() or {}

    # Members can remove themselves; admins can remove anyone
    if member_id != uid and gd.get("admin_id") != uid:
        caller_docs = list(
            db.collection(C.GROUP_MEMBERS)
            .where("group_id", "==", group_id)
            .where("user_id", "==", uid)
            .limit(1).get()
        )
        if not caller_docs or caller_docs[0].to_dict().get("role", "").upper() not in ("OWNER", "ADMIN"):
            return jsonify({"error": "Only group admins can remove other members"}), 403

    if member_id == gd.get("admin_id"):
        return jsonify({"error": "Cannot remove the group owner"}), 400

    # Delete from GROUP_MEMBERS
    gm_docs = list(
        db.collection(C.GROUP_MEMBERS)
        .where("group_id", "==", group_id)
        .where("user_id", "==", member_id)
        .get()
    )
    for gm in gm_docs:
        gm.reference.delete()

    # Delete their chat preview
    try:
        (
            db.collection(C.USER_CHAT_PREVIEWS)
            .document(member_id)
            .collection(C.CHATS_SUBCOLLECTION)
            .document(group_id)
            .delete()
        )
    except Exception as exc:
        logging.warning("Could not delete chat preview for removed member: %s", exc)

    invalidate_user_payload("groups", member_id)
    invalidate_user_payload("groups_canonical", member_id)
    invalidate_user_payload("groups", uid)
    invalidate_user_payload("groups_canonical", uid)

    return jsonify({"group": _build_group(db, group_id)})


# ─── Group settings ───────────────────────────────────────────────────────────

_SETTINGS_FIELDS = [
    "name",
    "image",
    "restrictMoneyAfterLoanRequest",
    "requireAdminApprovalForLoans",
    "allowMemberStatementAccess",
    "allowDirectMemberAccountWithdrawals",
    "allowMembersToViewOtherMemberBalances",
]

_SETTINGS_DB_MAP = {
    "name": "name",
    "image": "image",
    "restrictMoneyAfterLoanRequest": "restrict_money_after_loan",
    "requireAdminApprovalForLoans": "require_admin_approval_loans",
    "allowMemberStatementAccess": "allow_member_statement_access",
    "allowDirectMemberAccountWithdrawals": "allow_direct_member_account_withdrawals",
    "allowMembersToViewOtherMemberBalances": "allow_members_to_view_other_member_balances",
}


@groups_bp.route("/<group_id>/settings", methods=["GET"])
@require_auth
def get_settings(group_id):
    uid = get_jwt_identity()
    db = get_db()
    group_doc = db.collection(C.GROUP_ACCOUNTS).document(group_id).get()
    if not group_doc.exists:
        return jsonify({"error": "Group not found"}), 404
    gd = group_doc.to_dict() or {}
    settings = {
        "name":                                 gd.get("name", ""),
        "image":                                gd.get("image", ""),
        "restrictMoneyAfterLoanRequest":        gd.get("restrict_money_after_loan", False),
        "requireAdminApprovalForLoans":         gd.get("require_admin_approval_loans", False),
        "allowMemberStatementAccess":           gd.get("allow_member_statement_access", False),
        "allowDirectMemberAccountWithdrawals":  gd.get("allow_direct_member_account_withdrawals", False),
        "allowMembersToViewOtherMemberBalances": gd.get("allow_members_to_view_other_member_balances", False),
    }
    return jsonify({"settings": settings})


@groups_bp.route("/<group_id>/settings", methods=["PUT"])
@require_auth
def update_settings(group_id):
    uid = get_jwt_identity()
    db = get_db()
    group_doc = db.collection(C.GROUP_ACCOUNTS).document(group_id).get()
    if not group_doc.exists:
        return jsonify({"error": "Group not found"}), 404
    gd = group_doc.to_dict() or {}

    if gd.get("admin_id") != uid:
        return jsonify({"error": "Only the group owner can update settings"}), 403

    data = request.get_json() or {}
    updates = {}
    for field in _SETTINGS_FIELDS:
        if field in data:
            db_key = _SETTINGS_DB_MAP[field]
            updates[db_key] = data[field]

    if not updates:
        return jsonify({"error": "No valid settings fields provided"}), 400

    try:
        db.collection(C.GROUP_ACCOUNTS).document(group_id).update(updates)
    except Exception as exc:
        logging.exception("Failed to update group settings: %s", exc)
        return jsonify({"error": "Failed to update settings"}), 500

    # Propagate name/image change to all members' chat previews
    preview_updates = {}
    if "name" in updates:
        preview_updates["name"] = updates["name"]
    if "image" in updates:
        preview_updates["image"] = updates["image"]

    if preview_updates:
        try:
            member_docs = db.collection(C.GROUP_MEMBERS).where("group_id", "==", group_id).get()
            for md in member_docs:
                mid = md.to_dict().get("user_id", "")
                if mid:
                    (
                        db.collection(C.USER_CHAT_PREVIEWS)
                        .document(mid)
                        .collection(C.CHATS_SUBCOLLECTION)
                        .document(group_id)
                        .set(preview_updates, merge=True)
                    )
        except Exception as exc:
            logging.warning("Could not propagate settings to previews: %s", exc)

    invalidate_user_payload("groups", uid)
    invalidate_user_payload("groups_canonical", uid)

    return jsonify({"group": _build_group(db, group_id)})
