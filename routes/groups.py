import logging

from flask import Blueprint, request, jsonify
from firebase_admin import messaging
from firebase_utils import get_db
from models import group_to_dict, group_member_to_dict, group_member_from_chats
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


def _preview_group_data(db, user_id, group_id):
    """Read a chat-preview doc for user_id/group_id.

    Checks both the new structure (USER_CHAT_PREVIEWS subcollection) and the
    legacy CHATS/{uid} map. Prefers whichever copy carries a groupMembers
    array — the new-structure doc often only holds last-message metadata
    while the legacy doc (written by Android) has the full member list."""
    if not user_id:
        return None
    candidate = None
    try:
        pdoc = (
            db.collection(C.USER_CHAT_PREVIEWS)
            .document(user_id)
            .collection(C.CHATS_SUBCOLLECTION)
            .document(group_id)
            .get()
        )
        if pdoc.exists:
            candidate = pdoc.to_dict() or {}
            if candidate.get("groupMembers"):
                return candidate
    except Exception:
        pass
    try:
        chats_doc = db.collection(C.CHATS).document(user_id).get()
        if chats_doc.exists:
            chat_group = (chats_doc.to_dict() or {}).get(group_id)
            if isinstance(chat_group, dict):
                if chat_group.get("groupMembers") or candidate is None:
                    return chat_group
    except Exception:
        pass
    return candidate


def _preview_members(preview_data):
    raw = (preview_data or {}).get("groupMembers", [])
    if isinstance(raw, list):
        return [group_member_from_chats(m) for m in raw if isinstance(m, dict)]
    return []


def _group_profile(db, group_id):
    """Read the Android canonical structure: GROUP_PROFILES/{groupId} profile
    doc and its active members subcollection. Returns (profile|None, members)."""
    profile = None
    members = []
    try:
        pdoc = db.collection(C.GROUP_PROFILES).document(group_id).get()
        if pdoc.exists:
            profile = pdoc.to_dict() or {}
    except Exception:
        pass
    try:
        mdocs = (
            db.collection(C.GROUP_PROFILES)
            .document(group_id)
            .collection(C.GP_MEMBERS)
            .where("status", "==", "active")
            .get()
        )
        for md in mdocs:
            d = md.to_dict() or {}
            image = d.get("image", "")
            members.append({
                "id":       d.get("userId", "") or md.id,
                "name":     d.get("name", ""),
                "email":    d.get("email", ""),
                "phoneNum": d.get("phoneNum", ""),
                "image":    image,
                "photoUrl": image,
                "role":     d.get("role", "") or "UNKNOWN_ROLE",
            })
    except Exception:
        pass
    return profile, members


def _build_group(db, group_id, uid=None):
    """Fetch a group and its members.

    Source priority (mirrors the Android app):
      1. GROUP_PROFILES/{gid} + members subcollection  — canonical new structure
      2. GroupAccounts + GroupMembers                  — backend-created groups
      3. Chat-preview docs                             — legacy fallback
    """
    profile, members = _group_profile(db, group_id)

    doc = db.collection(C.GROUP_ACCOUNTS).document(group_id).get()
    gd = doc.to_dict() if doc.exists else None

    group_data = profile if profile else gd
    if group_data is None:
        group_data = _preview_group_data(db, uid, group_id)
        if group_data is None:
            return None

    if not members and gd is not None:
        member_docs = db.collection(C.GROUP_MEMBERS).where("group_id", "==", group_id).get()
        members = [group_member_to_dict(md.to_dict()) for md in member_docs]

    # Legacy fallbacks: caller's preview doc, then the admin's preview doc.
    if not members:
        members = _preview_members(_preview_group_data(db, uid, group_id))
    if not members:
        admin_id = group_data.get("adminID", "") or group_data.get("admin_id", "")
        if admin_id and admin_id != uid:
            members = _preview_members(_preview_group_data(db, admin_id, group_id))

    return group_to_dict(group_id, group_data, members or None)


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
                for chat_key, group_data in chat_data.items():
                    if not isinstance(group_data, dict):
                        continue
                    if chat_key in seen_ids:
                        continue
                    actual_id = group_data.get("id", chat_key)
                    if actual_id in seen_ids:
                        continue
                    result.append(group_to_dict(actual_id, group_data))
                    seen_ids.add(actual_id)
                    seen_ids.add(chat_key)
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

    image_value = data.get("image", "")
    if image_value and not str(image_value).startswith(("http://", "https://")):
        return jsonify({"error": "image must be an uploaded URL; use POST /photos/upload first"}), 400

    group_data = {
        "name": name,
        "image": image_value,
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

    return jsonify({"group": _build_group(db, group_id, uid)}), 201


@groups_bp.route("/<group_id>", methods=["GET"])
@require_auth
def get_group(group_id):
    uid = get_jwt_identity()
    db = get_db()
    g_dict = _build_group(db, group_id, uid)
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
    return jsonify({"group": _build_group(db, group_id, uid)})


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

    return jsonify({"group": _build_group(db, group_id, uid)}), 201


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

    return jsonify({"group": _build_group(db, group_id, uid)})


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


def _resolve_group_admin(db, group_id):
    """Return (ga_exists, profile_exists, merged_group_data, admin_id).

    Android-created groups only have a GROUP_PROFILES doc; backend-created
    ones only have GroupAccounts. Settings must work for both."""
    ga_doc = db.collection(C.GROUP_ACCOUNTS).document(group_id).get()
    ga = ga_doc.to_dict() if ga_doc.exists else None
    profile = None
    try:
        pdoc = db.collection(C.GROUP_PROFILES).document(group_id).get()
        if pdoc.exists:
            profile = pdoc.to_dict() or {}
    except Exception:
        pass
    merged = profile or ga or {}
    admin_id = ""
    if ga:
        admin_id = ga.get("admin_id", "")
    if not admin_id and profile:
        admin_id = profile.get("adminID", "")
    return ga is not None, profile is not None, merged, admin_id


@groups_bp.route("/<group_id>/settings", methods=["GET"])
@require_auth
def get_settings(group_id):
    uid = get_jwt_identity()
    db = get_db()
    ga_exists, profile_exists, gd, _admin = _resolve_group_admin(db, group_id)
    if not ga_exists and not profile_exists:
        return jsonify({"error": "Group not found"}), 404

    def flag(camel, snake):
        if camel in gd:
            return bool(gd.get(camel, False))
        return bool(gd.get(snake, False))

    settings = {
        "name":                                 gd.get("name", ""),
        "image":                                gd.get("image", ""),
        "restrictMoneyAfterLoanRequest":        flag("restrictMoneyAfterLoanRequest", "restrict_money_after_loan"),
        "requireAdminApprovalForLoans":         flag("requireAdminApprovalForLoans", "require_admin_approval_loans"),
        "allowMemberStatementAccess":           flag("allowMemberStatementAccess", "allow_member_statement_access"),
        "allowDirectMemberAccountWithdrawals":  flag("allowDirectMemberAccountWithdrawals", "allow_direct_member_account_withdrawals"),
        "allowMembersToViewOtherMemberBalances": flag("allowMembersToViewOtherMemberBalances", "allow_members_to_view_other_member_balances"),
    }
    return jsonify({"settings": settings})


@groups_bp.route("/<group_id>/settings", methods=["PUT"])
@require_auth
def update_settings(group_id):
    uid = get_jwt_identity()
    db = get_db()
    ga_exists, profile_exists, gd, admin_id = _resolve_group_admin(db, group_id)
    if not ga_exists and not profile_exists:
        return jsonify({"error": "Group not found"}), 404

    if admin_id != uid:
        return jsonify({"error": "Only the group owner can update settings"}), 403

    data = request.get_json() or {}
    updates = {}
    for field in _SETTINGS_FIELDS:
        if field in data:
            db_key = _SETTINGS_DB_MAP[field]
            updates[db_key] = data[field]

    if not updates:
        return jsonify({"error": "No valid settings fields provided"}), 400

    image_value = updates.get("image")
    if image_value not in (None, "") and not str(image_value).startswith(("http://", "https://")):
        return jsonify({"error": "image must be an uploaded URL; use POST /photos/upload first"}), 400

    try:
        if ga_exists:
            db.collection(C.GROUP_ACCOUNTS).document(group_id).update(updates)
        # Android canonical structure: GROUP_PROFILES uses the camelCase
        # field names from GroupStructureService.profileMap.
        if profile_exists or not ga_exists:
            profile_updates = {
                field: data[field] for field in _SETTINGS_FIELDS if field in data
            }
            db.collection(C.GROUP_PROFILES).document(group_id).set(
                profile_updates, merge=True
            )
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
            member_ids = set()
            for md in db.collection(C.GROUP_MEMBERS).where("group_id", "==", group_id).get():
                mid = md.to_dict().get("user_id", "")
                if mid:
                    member_ids.add(mid)
            # Android canonical member list
            try:
                for md in (
                    db.collection(C.GROUP_PROFILES)
                    .document(group_id)
                    .collection(C.GP_MEMBERS)
                    .get()
                ):
                    mid = (md.to_dict() or {}).get("userId", "") or md.id
                    if mid:
                        member_ids.add(mid)
            except Exception:
                pass
            for mid in member_ids:
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

    return jsonify({"group": _build_group(db, group_id, uid)})
