from flask import Blueprint, request, jsonify
from firebase_utils import get_db
from models import customer_to_dict, customer_payment_to_dict
from auth_utils import require_auth, get_jwt_identity
import db_constants as C
import uuid
from datetime import datetime, timezone
from cache_utils import cached_is_member, get_cached_group_payload, set_cached_group_payload, invalidate_group_payload

customers_bp = Blueprint("customers", __name__, url_prefix="/groups/<group_id>/customers")


def _is_true(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _is_member(db, group_id, uid):
    # ── New backend: GroupAccounts + GroupMembers ──────────────────────────────
    doc = db.collection(C.GROUP_ACCOUNTS).document(group_id).get()
    if doc.exists:
        if doc.to_dict().get("admin_id") == uid:
            return True
        gm = list(
            db.collection(C.GROUP_MEMBERS)
            .where("group_id", "==", group_id)
            .where("user_id", "==", uid)
            .limit(1).get()
        )
        if gm:
            return True
    # ── Original project: CHATS/{uid} map contains the group_id key ───────────
    try:
        chats_doc = db.collection(C.CHATS).document(uid).get()
        if chats_doc.exists:
            chat_data = chats_doc.to_dict() or {}
            if group_id in chat_data and isinstance(chat_data[group_id], dict):
                return True
    except Exception:
        pass
    return False


@customers_bp.route("", methods=["GET"])
@require_auth
def list_customers(group_id):
    uid = get_jwt_identity()
    db = get_db()
    canonical_only = _is_true(request.args.get("canonical"))
    is_mem, _ = cached_is_member(group_id, uid, lambda: (_is_member(db, group_id, uid), None))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    cache_name = "customers_canonical" if canonical_only else "customers"
    cached_payload = get_cached_group_payload(cache_name, group_id)
    if cached_payload is not None:
        return jsonify(cached_payload)

    # ── Source 1: new backend — flat Customers collection with group_id field ─
    docs = db.collection(C.CUSTOMERS).where("group_id", "==", group_id).get()
    cust_map = {d.id: customer_to_dict(d.id, d.to_dict()) for d in docs}

    # ── Source 2: original project — Customers/{groupId}/customers subcollection
    if not canonical_only:
        try:
            orig_docs = (
                db.collection(C.CUSTOMERS)
                .document(group_id)
                .collection("customers")
                .get()
            )
            for d in orig_docs:
                if d.id not in cust_map:
                    cust_map[d.id] = customer_to_dict(d.id, d.to_dict())
        except Exception:
            pass

    customers = sorted(cust_map.values(), key=lambda c: c["name"])
    payload = {"customers": customers}
    set_cached_group_payload(cache_name, group_id, payload)
    return jsonify(payload)


@customers_bp.route("", methods=["POST"])
@require_auth
def create_customer(group_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = cached_is_member(group_id, uid, lambda: (_is_member(db, group_id, uid), None))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    data = request.get_json() or {}
    name = data.get("name", "").strip()
    if not name:
        return jsonify({"error": "name is required"}), 400

    cust_id = data.get("id") or str(uuid.uuid4())
    cust_data = {
        "group_id":        group_id,
        "name":            name,
        "phone":           data.get("phone", ""),
        "email":           data.get("email", ""),
        "address":         data.get("address", ""),
        "balance":         float(data.get("balance", 0)),
        "credit_limit":    float(data.get("creditLimit", 0) or data.get("credit_limit", 0) or 0),
        "notes":           data.get("notes", ""),
        "is_active":       bool(data.get("isActive", True) if "isActive" in data else data.get("is_active", True)),
        "created_by":      uid,
        "created_at":      int(datetime.now(timezone.utc).timestamp() * 1000),
        "tax_id":          data.get("taxId", "")         or data.get("tax_id",         ""),
        "secondary_phone": data.get("secondaryPhone", "") or data.get("secondary_phone", ""),
        "category":        data.get("category", ""),
    }
    db.collection(C.CUSTOMERS).document(cust_id).set(cust_data)

    # Also write to original format: Customers/{groupId}/customers/{custId}
    try:
        db.collection(C.CUSTOMERS).document(group_id).collection("customers").document(cust_id).set({
            "id":               cust_id,
            "name":             name,
            "phone":            cust_data["phone"],
            "email":            cust_data["email"],
            "address":          cust_data["address"],
            "balance":          cust_data["balance"],
            "totalDebt":        cust_data["balance"],
            "creditLimit":      cust_data["credit_limit"],
            "notes":            cust_data["notes"],
            "isActive":         cust_data["is_active"],
            "chatID":           group_id,
            "registrationDate": cust_data["created_at"],
            "createdBy":        uid,
        })
    except Exception:
        pass

    invalidate_group_payload("customers", group_id)
    invalidate_group_payload("customers_canonical", group_id)
    return jsonify({"customer": customer_to_dict(cust_id, cust_data)}), 201


@customers_bp.route("/<customer_id>", methods=["PUT"])
@require_auth
def update_customer(group_id, customer_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = cached_is_member(group_id, uid, lambda: (_is_member(db, group_id, uid), None))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    doc = db.collection(C.CUSTOMERS).document(customer_id).get()
    if not (doc.exists and doc.to_dict().get("group_id") == group_id):
        try:
            orig_ref = (
                db.collection(C.CUSTOMERS)
                .document(group_id)
                .collection("customers")
                .document(customer_id)
            )
            orig_doc = orig_ref.get()
            if orig_doc.exists:
                doc = orig_doc
            else:
                return jsonify({"error": "Customer not found"}), 404
        except Exception:
            return jsonify({"error": "Customer not found"}), 404

    data = request.get_json() or {}
    updates = {}
    for req_key, db_key in [
        ("name", "name"), ("phone", "phone"), ("email", "email"),
        ("address", "address"), ("notes", "notes"),
        ("taxId", "tax_id"), ("tax_id", "tax_id"),
        ("secondaryPhone", "secondary_phone"), ("secondary_phone", "secondary_phone"),
        ("category", "category"),
    ]:
        if req_key in data:
            updates[db_key] = data[req_key]
    if "balance" in data:
        updates["balance"] = float(data["balance"])
    if "creditLimit" in data:
        updates["credit_limit"] = float(data["creditLimit"])
    if "credit_limit" in data:
        updates["credit_limit"] = float(data["credit_limit"])
    if "isActive" in data:
        updates["is_active"] = bool(data["isActive"])
    if "is_active" in data:
        updates["is_active"] = bool(data["is_active"])

    doc.reference.update(updates)
    invalidate_group_payload("customers", group_id)
    invalidate_group_payload("customers_canonical", group_id)
    updated = doc.reference.get()
    return jsonify({"customer": customer_to_dict(updated.id, updated.to_dict())})


@customers_bp.route("/<customer_id>", methods=["DELETE"])
@require_auth
def delete_customer(group_id, customer_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = cached_is_member(group_id, uid, lambda: (_is_member(db, group_id, uid), None))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403
    doc = db.collection(C.CUSTOMERS).document(customer_id).get()
    if not doc.exists or doc.to_dict().get("group_id") != group_id:
        return jsonify({"error": "Customer not found"}), 404
    doc.reference.delete()
    invalidate_group_payload("customers", group_id)
    invalidate_group_payload("customers_canonical", group_id)
    return jsonify({"message": "Customer deleted"})


@customers_bp.route("/<customer_id>/payments", methods=["POST"])
@require_auth
def record_payment(group_id, customer_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = cached_is_member(group_id, uid, lambda: (_is_member(db, group_id, uid), None))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    cust_doc = db.collection(C.CUSTOMERS).document(customer_id).get()
    if not (cust_doc.exists and cust_doc.to_dict().get("group_id") == group_id):
        try:
            orig_ref = (
                db.collection(C.CUSTOMERS)
                .document(group_id)
                .collection("customers")
                .document(customer_id)
            )
            orig_cust = orig_ref.get()
            if orig_cust.exists:
                cust_doc = orig_cust
            else:
                return jsonify({"error": "Customer not found"}), 404
        except Exception:
            return jsonify({"error": "Customer not found"}), 404

    data = request.get_json() or {}
    amount = float(data.get("amount", 0))
    if amount <= 0:
        return jsonify({"error": "amount must be positive"}), 400

    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    payment_id = str(uuid.uuid4())
    payment_data = {
        "group_id":    group_id,
        "customer_id": customer_id,
        "amount":      amount,
        "method":      data.get("method", "cash"),
        "notes":       data.get("notes", "") or "",
        "created_by":  uid,
        "is_allocated": True,
        "timestamp":   now,
    }

    # Write to flat CUSTOMER_PAYMENTS collection (new backend format)
    db.collection(C.CUSTOMER_PAYMENTS).document(payment_id).set(payment_data)

    # Also write to Windows Flutter format: CustomerPayments/{groupId} map document
    try:
        db.collection(C.CUSTOMER_PAYMENTS).document(group_id).set({
            payment_id: {
                "id":          payment_id,
                "customerId":  customer_id,
                "amount":      amount,
                "method":      data.get("method", "cash"),
                "notes":       data.get("notes", "") or "",
                "timestamp":   now,
                "createdBy":   uid,
                "isAllocated": True,
            }
        }, merge=True)
    except Exception:
        pass

    # Also write to Android format
    try:
        db.collection(C.CUSTOMER_PAYMENTS).document(group_id) \
            .collection("customers").document(customer_id) \
            .collection("payments").document(payment_id).set({
                "id":            payment_id,
                "customerId":    customer_id,
                "chatID":        group_id,
                "amount":        amount,
                "paymentMethod": data.get("method", "cash"),
                "reference":     data.get("reference", "") or "",
                "notes":         data.get("notes", "") or "",
                "paymentDate":   now,
                "recordedBy":    uid,
            })
    except Exception:
        pass

    # Reduce customer balance
    cust_dict = cust_doc.to_dict() or {}
    old_balance    = float(cust_dict.get("balance")   or cust_dict.get("totalDebt") or 0.0)
    old_total_paid = float(cust_dict.get("totalPaid", 0.0) or 0.0)
    new_balance    = max(0.0, old_balance - amount)
    new_total_paid = old_total_paid + amount
    cust_doc.reference.update({
        "balance":   new_balance,
        "totalDebt": new_balance,
        "totalPaid": new_total_paid,
    })

    invalidate_group_payload("customers", group_id)
    invalidate_group_payload("customers_canonical", group_id)
    updated_cust = cust_doc.reference.get()
    return jsonify({
        "payment": customer_payment_to_dict(payment_id, payment_data),
        "customer": customer_to_dict(updated_cust.id, updated_cust.to_dict()),
    }), 201


@customers_bp.route("/<customer_id>/payments", methods=["GET"])
@require_auth
def list_payments(group_id, customer_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = cached_is_member(group_id, uid, lambda: (_is_member(db, group_id, uid), None))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    pay_map = {}

    # ── Source 1: flat CustomerPayments collection (new backend) ──────────────
    try:
        docs = (
            db.collection(C.CUSTOMER_PAYMENTS)
            .where("customer_id", "==", customer_id)
            .get()
        )
        for d in docs:
            pay_map[d.id] = customer_payment_to_dict(d.id, d.to_dict())
    except Exception:
        pass

    # ── Source 2: Windows Flutter format — CustomerPayments/{groupId} map doc ──
    try:
        win_doc = db.collection(C.CUSTOMER_PAYMENTS).document(group_id).get()
        if win_doc.exists:
            for pay_id, val in (win_doc.to_dict() or {}).items():
                if isinstance(val, dict) and val.get("customerId") == customer_id:
                    if pay_id not in pay_map:
                        pay_map[pay_id] = customer_payment_to_dict(pay_id, {
                            "customer_id": val.get("customerId", customer_id),
                            "amount":      val.get("amount", 0.0),
                            "method":      val.get("method", "cash"),
                            "notes":       val.get("notes", ""),
                            "created_by":  val.get("createdBy", ""),
                            "is_allocated": val.get("isAllocated", True),
                            "timestamp":   val.get("timestamp", 0),
                        })
    except Exception:
        pass

    # ── Source 3: Android format ───────────────────────────────────────────────
    try:
        android_docs = (
            db.collection(C.CUSTOMER_PAYMENTS)
            .document(group_id)
            .collection("customers")
            .document(customer_id)
            .collection("payments")
            .get()
        )
        for d in android_docs:
            if d.id not in pay_map:
                ad = d.to_dict() or {}
                pay_map[d.id] = customer_payment_to_dict(d.id, {
                    "customer_id": ad.get("customerId", customer_id),
                    "amount":      ad.get("amount", 0.0),
                    "method":      ad.get("paymentMethod", "cash"),
                    "notes":       ad.get("notes", ""),
                    "created_by":  ad.get("recordedBy", ""),
                    "is_allocated": True,
                    "timestamp":   ad.get("paymentDate", 0),
                })
    except Exception:
        pass

    payments = sorted(pay_map.values(), key=lambda p: p["timestamp"], reverse=True)
    return jsonify({"payments": payments})
