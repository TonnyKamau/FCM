from flask import Blueprint, request, jsonify
from firebase_utils import get_db
from models import group_to_dict, group_member_to_dict, group_member_from_chats
from auth_utils import require_auth, get_jwt_identity
import db_constants as C
import uuid
from datetime import datetime, timezone

groups_bp = Blueprint("groups", __name__, url_prefix="/groups")


def _now_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _build_group(db, group_id):
    """Fetch a group document and its members; returns group dict or None."""
    doc = db.collection(C.GROUP_ACCOUNTS).document(group_id).get()
    if not doc.exists:
        return None
    gd = doc.to_dict()

    member_docs = db.collection(C.GROUP_MEMBERS).where("group_id", "==", group_id).get()
    members = []
    for md in member_docs:
        m = md.to_dict()
        user_doc = db.collection(C.USERS).document(m.get("user_id", "")).get()
        u = user_doc.to_dict() if user_doc.exists else {}
        members.append(group_member_to_dict(m, u))

    return group_to_dict(doc.id, gd, members)


@groups_bp.route("", methods=["GET"])
@require_auth
def list_groups():
    uid = get_jwt_identity()
    db  = get_db()

    result     = []
    seen_ids   = set()

    # ── Source 1: new backend — GroupMembers collection ───────────────────────
    member_docs = db.collection(C.GROUP_MEMBERS).where("user_id", "==", uid).get()
    group_ids = set(
        m.to_dict().get("group_id") for m in member_docs
        if m.to_dict().get("group_id")
    )

    # ── Source 2: new backend — GroupAccounts the user owns ───────────────────
    owned_docs = db.collection(C.GROUP_ACCOUNTS).where("admin_id", "==", uid).get()
    for d in owned_docs:
        group_ids.add(d.id)

    for gid in group_ids:
        g_dict = _build_group(db, gid)
        if g_dict:
            result.append(g_dict)
            seen_ids.add(gid)

    # ── Source 3: original project — CHATS/{uid} map document ─────────────────
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

    result.sort(key=lambda g: g.get("timestamp", 0), reverse=True)
    return jsonify({"groups": result})


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

    # Add creator as OWNER member
    db.collection(C.GROUP_MEMBERS).document(str(uuid.uuid4())).set({
        "group_id": group_id,
        "user_id": uid,
        "role": "OWNER",
    })

    # Add extra members provided in request
    for m in data.get("members", []):
        email = m.get("email", "").strip().lower()
        if email:
            user_docs = list(
                db.collection(C.USERS).where("email", "==", email).limit(1).get()
            )
            if user_docs and user_docs[0].id != uid:
                db.collection(C.GROUP_MEMBERS).document(str(uuid.uuid4())).set({
                    "group_id": group_id,
                    "user_id": user_docs[0].id,
                    "role": m.get("role", "member"),
                })

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
    return jsonify({"group": _build_group(db, group_id)})
