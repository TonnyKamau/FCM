from flask import Blueprint, request, jsonify
from firebase_utils import get_db
from models import customer_to_dict, customer_payment_to_dict, sale_to_dict
from auth_utils import require_auth, get_jwt_identity
import db_constants as C
import uuid
from datetime import datetime, timezone
from cache_utils import cached_is_member, get_cached_group_payload, set_cached_group_payload, invalidate_group_payload
from routes.messages import post_group_event_message

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
    # ── New structure: USER_CHAT_PREVIEWS/{uid}/CHATS/{group_id} ─────────────
    try:
        preview_doc = (
            db.collection(C.USER_CHAT_PREVIEWS)
            .document(uid)
            .collection(C.CHATS_SUBCOLLECTION)
            .document(group_id)
            .get()
        )
        if preview_doc.exists:
            return True
    except Exception:
        pass
    # ── Legacy: CHATS/{uid} map ───────────────────────────────────────────────
    try:
        chats_doc = db.collection(C.CHATS).document(uid).get()
        if chats_doc.exists:
            chat_data = chats_doc.to_dict() or {}
            if group_id in chat_data and isinstance(chat_data[group_id], dict):
                return True
    except Exception:
        pass
    return False


def _bd_customers_ref(db, group_id):
    """BUSINESS_DATA/{groupId}/customers subcollection."""
    return (
        db.collection(C.BUSINESS_DATA)
        .document(group_id)
        .collection(C.BD_CUSTOMERS)
    )


def _bd_payments_ref(db, group_id):
    """BUSINESS_DATA/{groupId}/customer_payments subcollection."""
    return (
        db.collection(C.BUSINESS_DATA)
        .document(group_id)
        .collection(C.BD_CUSTOMER_PAYMENTS)
    )


def _bd_sales_ref(db, group_id):
    """BUSINESS_DATA/{groupId}/sales subcollection."""
    return (
        db.collection(C.BUSINESS_DATA)
        .document(group_id)
        .collection(C.BD_SALES)
    )


def _resolve_customer(db, group_id, customer_id):
    """
    Return (doc_ref, doc_snapshot) for a customer across Android, flat, and legacy paths.
    Returns (None, None) if not found.
    """
    # Android path
    bd_ref = _bd_customers_ref(db, group_id).document(customer_id)
    bd_doc = bd_ref.get()
    if bd_doc.exists:
        return bd_ref, bd_doc

    # Flat Customers collection
    flat_doc = db.collection(C.CUSTOMERS).document(customer_id).get()
    if flat_doc.exists and flat_doc.to_dict().get("group_id") == group_id:
        return flat_doc.reference, flat_doc

    # Original nested format
    try:
        orig_ref = (
            db.collection(C.CUSTOMERS)
            .document(group_id)
            .collection("customers")
            .document(customer_id)
        )
        orig_doc = orig_ref.get()
        if orig_doc.exists:
            return orig_ref, orig_doc
    except Exception:
        pass

    return None, None


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

    cust_map = {}

    # ── Source 1: Android path — BUSINESS_DATA/{groupId}/customers ───────────
    try:
        for d in _bd_customers_ref(db, group_id).get():
            cust_map[d.id] = customer_to_dict(d.id, d.to_dict())
    except Exception:
        pass

    # ── Source 2: flat Customers collection ───────────────────────────────────
    try:
        docs = db.collection(C.CUSTOMERS).where("group_id", "==", group_id).get()
        for d in docs:
            if d.id not in cust_map:
                cust_map[d.id] = customer_to_dict(d.id, d.to_dict())
    except Exception:
        pass

    # ── Source 3: original — Customers/{groupId}/customers subcollection ──────
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


@customers_bp.route("/<customer_id>", methods=["GET"])
@require_auth
def get_customer(group_id, customer_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = cached_is_member(group_id, uid, lambda: (_is_member(db, group_id, uid), None))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    ref, doc = _resolve_customer(db, group_id, customer_id)
    if doc is None:
        return jsonify({"error": "Customer not found"}), 404

    return jsonify({"customer": customer_to_dict(doc.id, doc.to_dict())})


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
        "groupId":         group_id,
        "chatID":          group_id,
        "id":              cust_id,
        "name":            name,
        "phone":           data.get("phone", ""),
        "email":           data.get("email", ""),
        "address":         data.get("address", ""),
        "balance":         float(data.get("balance", 0)),
        "totalCredit":     float(data.get("totalCredit", data.get("balance", 0)) or 0),
        "totalPaid":       float(data.get("totalPaid", 0) or 0),
        "credit_limit":    float(data.get("creditLimit", 0) or data.get("credit_limit", 0) or 0),
        "notes":           data.get("notes", ""),
        "is_active":       bool(data.get("isActive", True) if "isActive" in data else data.get("is_active", True)),
        "isActive":        bool(data.get("isActive", True) if "isActive" in data else data.get("is_active", True)),
        "created_by":      uid,
        "created_at":      int(datetime.now(timezone.utc).timestamp() * 1000),
        "registrationDate": int(datetime.now(timezone.utc).timestamp() * 1000),
        "tax_id":          data.get("taxId", "")         or data.get("tax_id",         ""),
        "kraPin":          data.get("taxId", "")         or data.get("tax_id",         ""),
        "secondary_phone": data.get("secondaryPhone", "") or data.get("secondary_phone", ""),
        "category":        data.get("category", ""),
    }

    # ── Primary: Android path BUSINESS_DATA/{groupId}/customers/{custId} ─────
    _bd_customers_ref(db, group_id).document(cust_id).set(cust_data)

    # ── Legacy: flat Customers collection ────────────────────────────────────
    try:
        db.collection(C.CUSTOMERS).document(cust_id).set(cust_data)
    except Exception:
        pass

    # ── Legacy: original nested format Customers/{groupId}/customers/{custId} ─
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
    try:
        post_group_event_message(
            db,
            uid,
            group_id,
            f"📝 New Customer Registered: {name} | Phone: {cust_data['phone']}",
        )
    except Exception:
        pass
    return jsonify({"customer": customer_to_dict(cust_id, cust_data)}), 201


@customers_bp.route("/<customer_id>", methods=["PUT"])
@require_auth
def update_customer(group_id, customer_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = cached_is_member(group_id, uid, lambda: (_is_member(db, group_id, uid), None))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    ref, doc = _resolve_customer(db, group_id, customer_id)
    if doc is None:
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
    if "taxId" in data or "tax_id" in data:
        updates["kraPin"] = data.get("taxId", data.get("tax_id", ""))
    if "balance" in data:
        updates["balance"] = float(data["balance"])
    if "creditLimit" in data:
        updates["credit_limit"] = float(data["creditLimit"])
    if "credit_limit" in data:
        updates["credit_limit"] = float(data["credit_limit"])
    if "isActive" in data:
        updates["is_active"] = bool(data["isActive"])
        updates["isActive"] = bool(data["isActive"])
    if "is_active" in data:
        updates["is_active"] = bool(data["is_active"])

    doc.reference.update(updates)

    # Mirror to Android path if primary was not Android
    try:
        bd_ref = _bd_customers_ref(db, group_id).document(customer_id)
        bd_doc = bd_ref.get()
        if bd_doc.exists:
            bd_ref.update(updates)
        else:
            bd_ref.set({**doc.to_dict(), **updates}, merge=True)
    except Exception:
        pass

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

    ref, doc = _resolve_customer(db, group_id, customer_id)
    if doc is None:
        return jsonify({"error": "Customer not found"}), 404

    doc.reference.delete()

    # Also delete from Android path if different
    try:
        bd_ref = _bd_customers_ref(db, group_id).document(customer_id)
        if bd_ref != ref:
            bd_ref.delete()
    except Exception:
        pass

    invalidate_group_payload("customers", group_id)
    invalidate_group_payload("customers_canonical", group_id)
    return jsonify({"message": "Customer deleted"})


@customers_bp.route("/<customer_id>/sales", methods=["GET"])
@require_auth
def customer_sales(group_id, customer_id):
    """
    Return all credit sales attributed to this customer.

    Reads from BUSINESS_DATA/{groupId}/sales (primary) and flat CREDIT_SALE collection.
    """
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = cached_is_member(group_id, uid, lambda: (_is_member(db, group_id, uid), None))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    ref, cust_doc = _resolve_customer(db, group_id, customer_id)
    if cust_doc is None:
        return jsonify({"error": "Customer not found"}), 404

    sale_map = {}

    # ── Source 1: Android BUSINESS_DATA/{groupId}/sales ──────────────────────
    try:
        for d in (
            _bd_sales_ref(db, group_id)
            .where("customer_id", "==", customer_id)
            .get()
        ):
            sale_map[d.id] = sale_to_dict(d.id, d.to_dict())
    except Exception:
        pass

    # ── Source 2: flat CREDIT_SALE collection ─────────────────────────────────
    try:
        for d in (
            db.collection(C.CREDIT_SALE)
            .where("group_id", "==", group_id)
            .where("customer_id", "==", customer_id)
            .get()
        ):
            if d.id not in sale_map:
                sale_map[d.id] = sale_to_dict(d.id, d.to_dict())
    except Exception:
        pass

    # Also include customerId (camelCase) variant in case old data uses it
    try:
        for d in (
            db.collection(C.CREDIT_SALE)
            .where("group_id", "==", group_id)
            .where("customerId", "==", customer_id)
            .get()
        ):
            if d.id not in sale_map:
                sale_map[d.id] = sale_to_dict(d.id, d.to_dict())
    except Exception:
        pass

    sales = sorted(sale_map.values(), key=lambda s: s["date"], reverse=True)
    return jsonify({"sales": sales, "count": len(sales)})


@customers_bp.route("/<customer_id>/payments", methods=["POST"])
@require_auth
def record_payment(group_id, customer_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = cached_is_member(group_id, uid, lambda: (_is_member(db, group_id, uid), None))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    ref, cust_doc = _resolve_customer(db, group_id, customer_id)
    if cust_doc is None:
        return jsonify({"error": "Customer not found"}), 404

    data = request.get_json() or {}
    amount = float(data.get("amount", 0))
    if amount <= 0:
        return jsonify({"error": "amount must be positive"}), 400

    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    payment_id = str(uuid.uuid4())
    payment_data = {
        "group_id":     group_id,
        "groupId":      group_id,
        "id":           payment_id,
        "customer_id":  customer_id,
        "customerId":   customer_id,
        "amount":       amount,
        "method":       data.get("method", "cash"),
        "paymentMethod": data.get("method", "cash"),
        "reference":    data.get("reference", "") or "",
        "notes":        data.get("notes", "") or "",
        "created_by":   uid,
        "recordedBy":   uid,
        "is_allocated": True,
        "timestamp":    now,
        "paymentDate":  now,
        "entryType":    "customer_payment",
    }

    # ── Primary: Android path BUSINESS_DATA/{groupId}/customer_payments/{id} ──
    _bd_payments_ref(db, group_id).document(payment_id).set(payment_data)

    # ── Legacy: flat CustomerPayments collection ───────────────────────────────
    try:
        db.collection(C.CUSTOMER_PAYMENTS).document(payment_id).set(payment_data)
    except Exception:
        pass

    # ── Legacy: Windows Flutter format — CustomerPayments/{groupId} map doc ───
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

    # ── Legacy: Android subcollection format ─────────────────────────────────
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
    balance_update = {
        "balance":   new_balance,
        "totalDebt": new_balance,
        "totalPaid": new_total_paid,
    }
    cust_doc.reference.update(balance_update)

    # Mirror balance update to Android path
    try:
        bd_cust_ref = _bd_customers_ref(db, group_id).document(customer_id)
        bd_cust_doc = bd_cust_ref.get()
        if bd_cust_doc.exists and bd_cust_ref != ref:
            bd_cust_ref.update(balance_update)
    except Exception:
        pass

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

    # ── Source 1: Android path BUSINESS_DATA/{groupId}/customer_payments ─────
    try:
        for d in (
            _bd_payments_ref(db, group_id)
            .where("customerId", "==", customer_id)
            .get()
        ):
            pay_map[d.id] = customer_payment_to_dict(d.id, d.to_dict())
    except Exception:
        pass

    # ── Source 2: flat CustomerPayments collection (new backend) ──────────────
    try:
        docs = (
            db.collection(C.CUSTOMER_PAYMENTS)
            .where("customer_id", "==", customer_id)
            .get()
        )
        for d in docs:
            if d.id not in pay_map:
                pay_map[d.id] = customer_payment_to_dict(d.id, d.to_dict())
    except Exception:
        pass

    # ── Source 3: Windows Flutter format — CustomerPayments/{groupId} map doc ──
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

    # ── Source 4: Android subcollection format ─────────────────────────────────
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
