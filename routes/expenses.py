"""
Expenses & Income CRUD routes for the kit-ifms Flutter app.

Both Expenses and Income are stored in the same Firestore EXPENSES collection;
they are distinguished by the boolean field  is_expense  (True = expense,
False = income).  This mirrors the exact behaviour of the main kit-ifms
Flask backend so the Flutter app works unchanged.

Dual-source read strategy
--------------------------
Source 1 — new backend flat docs  : EXPENSES/{auto-id}  with  group_id  field
Source 2 — original kitifms format: EXPENSES/{adminId}  map document where
           each key is an entry ID and entries have  chatID == group_id.
"""

from flask import Blueprint, request, jsonify
from firebase_utils import get_db
from auth_utils import require_auth, get_jwt_identity
import uuid
from datetime import datetime, timezone
from cache_utils import cached_is_member, get_cached_group_payload, set_cached_group_payload, invalidate_group_payload

# ── Firestore collection names ────────────────────────────────────────────────
_EXPENSES       = "EXPENSES"
_GROUP_ACCOUNTS = "GroupAccounts"
_GROUP_MEMBERS  = "GroupMembers"
_CHATS          = "CHATS"

# ── Blueprints ────────────────────────────────────────────────────────────────
expenses_bp = Blueprint("expenses", __name__, url_prefix="/groups/<group_id>/expenses")
income_bp   = Blueprint("income",   __name__, url_prefix="/groups/<group_id>/income")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _is_true(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _to_dict(doc_id, d):
    """Serialise a raw Firestore dict to the JSON shape expected by Flutter."""
    is_expense = d.get("is_expense", True) if "is_expense" in d else d.get("isExpense", True)
    group_id   = d.get("group_id", "") or d.get("chatID", "")
    pay_method = d.get("payment_method", "") or d.get("paymentMethod", "") or "cash"
    return {
        "id":            doc_id,
        "chatID":        group_id,
        "name":          d.get("name", ""),
        "price":         float(d.get("price", 0) or 0),
        "isExpense":     is_expense,
        "category":      d.get("category", "Other"),
        "paymentMethod": pay_method,
        "notes":         d.get("notes", ""),
        "createdBy":     d.get("created_by", "") or d.get("createdBy", ""),
        "timestamp":     d.get("timestamp", _now_ms()),
        "createdAt":     d.get("created_at", "") or d.get("createdAt", ""),
    }


def _check_member(db, group_id, uid):
    """Returns (is_member: bool, admin_id: str | None)."""
    # ── New backend: GroupAccounts + GroupMembers ─────────────────────────────
    doc = db.collection(_GROUP_ACCOUNTS).document(group_id).get()
    if doc.exists:
        gd = doc.to_dict()
        if gd.get("admin_id") == uid:
            return True, gd.get("admin_id")
        gm = list(
            db.collection(_GROUP_MEMBERS)
            .where("group_id", "==", group_id)
            .where("user_id", "==", uid)
            .limit(1).get()
        )
        if gm:
            return True, gd.get("admin_id")
    # ── Original project: CHATS/{uid} map contains the group_id key ──────────
    try:
        chats_doc = db.collection(_CHATS).document(uid).get()
        if chats_doc.exists:
            chat_data = chats_doc.to_dict() or {}
            if group_id in chat_data and isinstance(chat_data[group_id], dict):
                gdata    = chat_data[group_id]
                admin_id = gdata.get("adminID", "") or gdata.get("admin_id", "") or None
                return True, admin_id
    except Exception:
        pass
    return False, None


# ── Shared business logic ─────────────────────────────────────────────────────

def _list(group_id, is_expense_flag):
    uid = get_jwt_identity()
    db  = get_db()
    canonical_only = _is_true(request.args.get("canonical"))
    is_mem, admin_id = cached_is_member(group_id, uid, lambda: _check_member(db, group_id, uid))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    cache_name = "expenses" if is_expense_flag else "income"
    if canonical_only:
        cache_name = f"{cache_name}_canonical"
    cached_payload = get_cached_group_payload(cache_name, group_id)
    if cached_payload is not None:
        return jsonify(cached_payload)

    # Source 1: flat EXPENSES collection
    docs = (
        db.collection(_EXPENSES)
        .where("group_id", "==", group_id)
        .where("is_expense", "==", is_expense_flag)
        .get()
    )
    entry_map = {d.id: _to_dict(d.id, d.to_dict()) for d in docs}

    # Source 2: original kitifms — EXPENSES/{adminId} single map document
    if admin_id and not canonical_only:
        try:
            orig_doc = db.collection(_EXPENSES).document(admin_id).get()
            if orig_doc.exists:
                for entry_id, entry_data in (orig_doc.to_dict() or {}).items():
                    if not isinstance(entry_data, dict) or entry_id in entry_map:
                        continue
                    chat_id = entry_data.get("chatID", "") or entry_data.get("group_id", "")
                    is_exp  = (entry_data.get("isExpense", True) if "isExpense" in entry_data
                               else entry_data.get("is_expense", True))
                    if chat_id != group_id or is_exp != is_expense_flag:
                        continue
                    entry_map[entry_id] = _to_dict(entry_id, entry_data)
        except Exception:
            pass

    rows = sorted(entry_map.values(), key=lambda r: r["timestamp"], reverse=True)
    key  = "expenses" if is_expense_flag else "incomes"
    payload = {key: rows}
    set_cached_group_payload(cache_name, group_id, payload)
    return jsonify(payload)


def _create(group_id, is_expense_flag):
    uid = get_jwt_identity()
    db  = get_db()
    is_mem, admin_id = cached_is_member(group_id, uid, lambda: _check_member(db, group_id, uid))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    data = request.get_json() or {}
    name = data.get("name", "").strip()
    if not name:
        return jsonify({"error": "name is required"}), 400

    admin_id = admin_id or uid
    now      = _now_ms()
    entry_id = data.get("id") or str(uuid.uuid4())
    entry_data = {
        "group_id":       group_id,
        "admin_id":       admin_id,
        "name":           name,
        "price":          float(data.get("price", 0)),
        "is_expense":     is_expense_flag,
        "category":       data.get("category", "Other"),
        "payment_method": (data.get("paymentMethod") or data.get("payment_method") or "cash"),
        "notes":          data.get("notes", ""),
        "created_by":     uid,
        "timestamp":      int(data.get("timestamp", now)),
        "created_at":     datetime.now(timezone.utc).isoformat(),
    }
    db.collection(_EXPENSES).document(entry_id).set(entry_data)
    base_cache_name = "expenses" if is_expense_flag else "income"
    invalidate_group_payload(base_cache_name, group_id)
    invalidate_group_payload(f"{base_cache_name}_canonical", group_id)
    key = "expense" if is_expense_flag else "income"
    return jsonify({key: _to_dict(entry_id, entry_data)}), 201


def _update(group_id, entry_id, is_expense_flag):
    uid = get_jwt_identity()
    db  = get_db()
    is_mem, _ = cached_is_member(group_id, uid, lambda: _check_member(db, group_id, uid))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    doc = db.collection(_EXPENSES).document(entry_id).get()
    if not doc.exists:
        return jsonify({"error": "Entry not found"}), 404
    d = doc.to_dict()
    if d.get("group_id") != group_id or d.get("is_expense") != is_expense_flag:
        return jsonify({"error": "Entry not found"}), 404

    data    = request.get_json() or {}
    updates = {}
    if "name"      in data: updates["name"]           = data["name"]
    if "price"     in data: updates["price"]          = float(data["price"])
    if "timestamp" in data: updates["timestamp"]      = int(data["timestamp"])
    if "category"  in data: updates["category"]       = data["category"]
    if "notes"     in data: updates["notes"]          = data["notes"]
    if "paymentMethod"  in data: updates["payment_method"] = data["paymentMethod"]
    if "payment_method" in data: updates["payment_method"] = data["payment_method"]
    doc.reference.update(updates)

    base_cache_name = "expenses" if is_expense_flag else "income"
    invalidate_group_payload(base_cache_name, group_id)
    invalidate_group_payload(f"{base_cache_name}_canonical", group_id)
    updated = db.collection(_EXPENSES).document(entry_id).get()
    key = "expense" if is_expense_flag else "income"
    return jsonify({key: _to_dict(updated.id, updated.to_dict())})


def _delete(group_id, entry_id, is_expense_flag):
    uid = get_jwt_identity()
    db  = get_db()
    is_mem, _ = cached_is_member(group_id, uid, lambda: _check_member(db, group_id, uid))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    doc = db.collection(_EXPENSES).document(entry_id).get()
    if not doc.exists:
        return jsonify({"error": "Entry not found"}), 404
    d = doc.to_dict()
    if d.get("group_id") != group_id or d.get("is_expense") != is_expense_flag:
        return jsonify({"error": "Entry not found"}), 404

    doc.reference.delete()
    base_cache_name = "expenses" if is_expense_flag else "income"
    invalidate_group_payload(base_cache_name, group_id)
    invalidate_group_payload(f"{base_cache_name}_canonical", group_id)
    return jsonify({"message": "Deleted"})


# ── Expense routes ────────────────────────────────────────────────────────────

@expenses_bp.route("", methods=["GET"])
@require_auth
def list_expenses(group_id):
    return _list(group_id, True)


@expenses_bp.route("", methods=["POST"])
@require_auth
def create_expense(group_id):
    return _create(group_id, True)


@expenses_bp.route("/<entry_id>", methods=["PUT"])
@require_auth
def update_expense(group_id, entry_id):
    return _update(group_id, entry_id, True)


@expenses_bp.route("/<entry_id>", methods=["DELETE"])
@require_auth
def delete_expense(group_id, entry_id):
    return _delete(group_id, entry_id, True)


# ── Income routes (same collection, is_expense=False) ────────────────────────

@income_bp.route("", methods=["GET"])
@require_auth
def list_income(group_id):
    return _list(group_id, False)


@income_bp.route("", methods=["POST"])
@require_auth
def create_income(group_id):
    return _create(group_id, False)


@income_bp.route("/<entry_id>", methods=["PUT"])
@require_auth
def update_income(group_id, entry_id):
    return _update(group_id, entry_id, False)


@income_bp.route("/<entry_id>", methods=["DELETE"])
@require_auth
def delete_income(group_id, entry_id):
    return _delete(group_id, entry_id, False)
