"""
Group Accounts routes — list/create group accounts and record transactions.

Paths
-----
GET  /groups/<group_id>/accounts
POST /groups/<group_id>/accounts
GET  /groups/<group_id>/accounts/<account_id>/transactions
POST /groups/<group_id>/accounts/<account_id>/deposit
"""

import logging
import uuid
from datetime import datetime, timezone

from flask import Blueprint, request, jsonify

from firebase_utils import get_db
from auth_utils import require_auth, get_jwt_identity
from google.cloud.firestore import Increment
import db_constants as C

group_accounts_bp = Blueprint("group_accounts", __name__, url_prefix="/groups/<group_id>/accounts")


def _now_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _is_member_or_admin(db, group_id, uid):
    """Return True if uid is the group admin or a member."""
    doc = db.collection(C.GROUP_ACCOUNTS).document(group_id).get()
    if not doc.exists:
        return False, None
    gd = doc.to_dict() or {}
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
    # Check chat preview (handles Android-created groups)
    try:
        preview = (
            db.collection(C.USER_CHAT_PREVIEWS)
            .document(uid)
            .collection(C.CHATS_SUBCOLLECTION)
            .document(group_id)
            .get()
        )
        if preview.exists:
            return True, gd
    except Exception:
        pass
    return False, None


# ── Accounts collection ────────────────────────────────────────────────────────

@group_accounts_bp.route("", methods=["GET"])
@require_auth
def list_accounts(group_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = _is_member_or_admin(db, group_id, uid)
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    try:
        account_docs = (
            db.collection(C.GROUP_ACCOUNTS)
            .document(group_id)
            .collection("accounts")
            .get()
        )
        accounts = []
        for doc in account_docs:
            d = doc.to_dict() or {}
            accounts.append({
                "id":            doc.id,
                "accountType":   d.get("accountType", "savings"),
                "accountNumber": d.get("accountNumber", ""),
                "balance":       float(d.get("balance", 0) or 0),
                "createdAt":     d.get("createdAt", 0),
                "groupId":       group_id,
            })
        return jsonify({"accounts": accounts})
    except Exception as exc:
        logging.exception("Failed to list group accounts: %s", exc)
        return jsonify({"error": "Failed to list accounts"}), 500


@group_accounts_bp.route("", methods=["POST"])
@require_auth
def create_account(group_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, gd = _is_member_or_admin(db, group_id, uid)
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403
    # Only admin can create accounts
    if gd and gd.get("admin_id") != uid:
        return jsonify({"error": "Only the group admin can create accounts"}), 403

    data = request.get_json() or {}
    account_type = data.get("accountType", "savings").lower()
    if account_type not in ("savings", "income"):
        return jsonify({"error": "accountType must be 'savings' or 'income'"}), 400

    account_id = str(uuid.uuid4())
    now = _now_ms()
    account_data = {
        "id":            account_id,
        "accountType":   account_type,
        "accountNumber": data.get("accountNumber", ""),
        "balance":       0.0,
        "createdAt":     now,
        "groupId":       group_id,
        "createdBy":     uid,
    }
    try:
        (
            db.collection(C.GROUP_ACCOUNTS)
            .document(group_id)
            .collection("accounts")
            .document(account_id)
            .set(account_data)
        )
    except Exception as exc:
        logging.exception("Failed to create group account: %s", exc)
        return jsonify({"error": "Failed to create account"}), 500

    return jsonify({"account": account_data}), 201


# ── Transactions ───────────────────────────────────────────────────────────────

@group_accounts_bp.route("/<account_id>/transactions", methods=["GET"])
@require_auth
def list_transactions(group_id, account_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = _is_member_or_admin(db, group_id, uid)
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    # Verify account exists
    account_ref = (
        db.collection(C.GROUP_ACCOUNTS)
        .document(group_id)
        .collection("accounts")
        .document(account_id)
    )
    if not account_ref.get().exists:
        return jsonify({"error": "Account not found"}), 404

    try:
        tx_docs = (
            account_ref.collection("transactions")
            .order_by("timestamp", direction="DESCENDING")
            .limit(200)
            .get()
        )
        transactions = []
        for doc in tx_docs:
            d = doc.to_dict() or {}
            transactions.append({
                "id":         doc.id,
                "type":       d.get("type", "deposit"),
                "amount":     float(d.get("amount", 0) or 0),
                "balance":    float(d.get("balance", 0) or 0),
                "memberId":   d.get("memberId", ""),
                "memberName": d.get("memberName", ""),
                "note":       d.get("note", ""),
                "timestamp":  d.get("timestamp", 0),
                "accountId":  account_id,
                "groupId":    group_id,
            })
        return jsonify({"transactions": transactions})
    except Exception as exc:
        logging.exception("Failed to list transactions: %s", exc)
        return jsonify({"error": "Failed to list transactions"}), 500


@group_accounts_bp.route("/<account_id>/deposit", methods=["POST"])
@require_auth
def deposit(group_id, account_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = _is_member_or_admin(db, group_id, uid)
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    data = request.get_json() or {}
    amount = data.get("amount")
    if amount is None:
        return jsonify({"error": "amount is required"}), 400
    try:
        amount = float(amount)
        if amount <= 0:
            raise ValueError("amount must be positive")
    except (TypeError, ValueError) as exc:
        return jsonify({"error": str(exc)}), 400

    member_id = data.get("memberId", uid)
    member_name = data.get("memberName", "")
    note = data.get("note", "")

    account_ref = (
        db.collection(C.GROUP_ACCOUNTS)
        .document(group_id)
        .collection("accounts")
        .document(account_id)
    )
    account_doc = account_ref.get()
    if not account_doc.exists:
        return jsonify({"error": "Account not found"}), 404

    now = _now_ms()
    tx_id = str(uuid.uuid4())

    try:
        # Increment balance atomically
        account_ref.update({"balance": Increment(amount)})

        # Fetch updated balance
        updated_doc = account_ref.get()
        new_balance = float((updated_doc.to_dict() or {}).get("balance", 0))

        tx_data = {
            "id":         tx_id,
            "type":       "deposit",
            "amount":     amount,
            "balance":    new_balance,
            "memberId":   member_id,
            "memberName": member_name,
            "note":       note,
            "timestamp":  now,
            "accountId":  account_id,
            "groupId":    group_id,
            "recordedBy": uid,
        }
        account_ref.collection("transactions").document(tx_id).set(tx_data)

        return jsonify({"transaction": tx_data, "newBalance": new_balance}), 201
    except Exception as exc:
        logging.exception("Failed to record deposit: %s", exc)
        return jsonify({"error": "Failed to record deposit"}), 500
