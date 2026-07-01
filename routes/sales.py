from flask import Blueprint, request, jsonify
from firebase_utils import get_db
from models import sale_to_dict
from auth_utils import require_auth, get_jwt_identity
import db_constants as C
import logging
import uuid
from datetime import datetime, timezone
from google.cloud.firestore import Increment
from cache_utils import (
    cached_is_member,
    get_cached_group_payload, set_cached_group_payload, invalidate_group_payload,
    invalidate_report, invalidate_products,
)

sales_bp = Blueprint("sales", __name__, url_prefix="/groups/<group_id>/sales")


def _is_true(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _increment_group_account_balance(db, group_id, amount):
    """Atomically add `amount` to the savings GroupAccount balance."""
    try:
        accounts_ref = (
            db.collection("GroupAccounts")
            .document(group_id)
            .collection("accounts")
        )
        savings_docs = list(
            accounts_ref.where("accountType", "==", "savings").limit(1).get()
        )
        if not savings_docs:
            savings_docs = list(
                accounts_ref.where("accountType", "==", "NORMAL").limit(1).get()
            )
        if savings_docs:
            savings_docs[0].reference.update({"balance": Increment(amount)})
    except Exception as e:
        logging.exception("_increment_group_account_balance error (%s): %s", group_id, e)


def _check_member(db, group_id, uid):
    """Returns (is_member: bool, admin_id: str|None)."""
    doc = db.collection(C.GROUP_ACCOUNTS).document(group_id).get()
    if doc.exists:
        gd = doc.to_dict()
        if gd.get("admin_id") == uid:
            return True, gd.get("admin_id")
        gm = list(
            db.collection(C.GROUP_MEMBERS)
            .where("group_id", "==", group_id)
            .where("user_id", "==", uid)
            .limit(1).get()
        )
        if gm:
            return True, gd.get("admin_id")
    try:
        preview_doc = (
            db.collection(C.USER_CHAT_PREVIEWS)
            .document(uid)
            .collection(C.CHATS_SUBCOLLECTION)
            .document(group_id)
            .get()
        )
        if preview_doc.exists:
            gdata    = preview_doc.to_dict() or {}
            admin_id = gdata.get("adminID", "") or gdata.get("admin_id", "") or None
            return True, admin_id
    except Exception:
        pass
    try:
        chats_doc = db.collection(C.CHATS).document(uid).get()
        if chats_doc.exists:
            chat_data = chats_doc.to_dict() or {}
            if group_id in chat_data and isinstance(chat_data[group_id], dict):
                gdata    = chat_data[group_id]
                admin_id = gdata.get("adminID", "") or gdata.get("admin_id", "") or None
                return True, admin_id
    except Exception:
        pass
    return False, None


def _bd_sales_ref(db, group_id):
    """BUSINESS_DATA/{groupId}/sales subcollection."""
    return (
        db.collection(C.BUSINESS_DATA)
        .document(group_id)
        .collection(C.BD_SALES)
    )


def _bd_stock_movements_ref(db, group_id):
    """BUSINESS_DATA/{groupId}/stock_movements subcollection."""
    return (
        db.collection(C.BUSINESS_DATA)
        .document(group_id)
        .collection(C.BD_STOCK_MOVEMENTS)
    )


def _invalidate_sales_caches(group_id):
    invalidate_group_payload("sales", group_id)
    invalidate_group_payload("sales_canonical", group_id)
    invalidate_report("sales", group_id)
    invalidate_report("sales_canonical", group_id)


def _post_sale_notification(db, uid, group_id, description, total, now):
    """Fire-and-forget: post a chat message and update previews after a sale."""
    try:
        user_doc = db.collection(C.USERS).document(uid).get()
        sender_name = user_doc.to_dict().get("name", "User") if user_doc.exists else "User"

        msg_id   = str(uuid.uuid4())
        msg_text = description

        db.collection(C.CHATS).document(group_id).collection(C.MESSAGES_SUBCOLLECTION).document(msg_id).set({
            "id":            msg_id,
            "senderID":      uid,
            "senderName":    sender_name,
            "receiverID":    "",
            "receiverName":  "",
            "chatID":        group_id,
            "message":       msg_text,
            "isGroup":       True,
            "isMoneyShared": False,
            "isImageShared": False,
            "isPoll":        False,
            "isLoanRequest": False,
            "money":         "",
            "image":         "",
            "caption":       "",
            "timestamp":     now,
        })

        last_msg = f"{sender_name}: {msg_text}"
        try:
            db.collection(C.GROUP_ACCOUNTS).document(group_id).update({
                "last_message": last_msg,
                "timestamp":    now,
            })
        except Exception:
            pass

        all_member_ids = {uid}
        gm_docs = db.collection(C.GROUP_MEMBERS).where("group_id", "==", group_id).get()
        for gm in gm_docs:
            mid = gm.to_dict().get("user_id", "")
            if mid:
                all_member_ids.add(mid)

        group_doc  = db.collection(C.GROUP_ACCOUNTS).document(group_id).get()
        group_info = group_doc.to_dict() if group_doc.exists else {}

        chat_preview_base = {
            "id":            group_id,
            "name":          group_info.get("name", ""),
            "image":         group_info.get("image", ""),
            "lastMessage":   last_msg,
            "timestamp":     now,
            "isGroup":       True,
            "adminID":       group_info.get("admin_id", ""),
            "userID":        uid,
            "isMoneyShared": False,
            "isImageShared": False,
            "isVoiceNote":   False,
            "whoShared":     sender_name,
            "money":         "",
        }
        for member_uid in all_member_ids:
            preview_ref = (
                db.collection(C.USER_CHAT_PREVIEWS)
                .document(member_uid)
                .collection(C.CHATS_SUBCOLLECTION)
                .document(group_id)
            )
            preview_data = dict(chat_preview_base)
            preview_data["unreadCount"] = 0 if member_uid == uid else Increment(1)
            preview_ref.set(preview_data, merge=True)
    except Exception:
        pass  # notification failure must never block the sale response


def _build_stock_card(db, group_id, items, now):
    """Build a formatted stock card message mirroring the original app."""
    dt_now = datetime.now(timezone.utc)
    start_of_day = int(
        datetime(dt_now.year, dt_now.month, dt_now.day, tzinfo=timezone.utc)
        .timestamp() * 1000
    )
    lines = ["📊 STOCK CARD", "━" * 28]
    for item in items:
        prod_name = item.get("productName", "")
        prod_id   = item.get("productId",   "")
        if not prod_name:
            continue
        seen: dict = {}
        for coll in [C.CASH_SALE, C.CREDIT_SALE]:
            try:
                for d in db.collection(coll).where("group_id", "==", group_id).get():
                    dd = d.to_dict() or {}
                    if (dd.get("date", 0) or 0) >= start_of_day:
                        p = dd.get("product_name") or dd.get("name", "")
                        if p == prod_name and d.id not in seen:
                            seen[d.id] = int(dd.get("quantity", 0) or 0)
            except Exception as e:
                logging.exception("stock_card flat query (%s): %s", coll, e)
        closing_bal = 0
        try:
            prod_doc = db.collection(C.PRODUCTS).document(prod_id).get()
            if prod_doc.exists:
                closing_bal = int(prod_doc.to_dict().get("available_stock", 0) or 0)
            else:
                orig_prods = db.collection(C.PRODUCTS).document(group_id).get()
                if orig_prods.exists:
                    pdata = (orig_prods.to_dict() or {}).get(prod_id, {})
                    if isinstance(pdata, dict):
                        closing_bal = int(
                            pdata.get("available_stock") or
                            pdata.get("availableStock") or 0
                        )
        except Exception as e:
            logging.exception("stock_card closing_bal (%s): %s", prod_id, e)
        lines.append(f"{'Product'.ljust(15)} | {prod_name}")
        lines.append("━" * 28)
        lines.append(f"{'Sold Today'.ljust(15)} | {sum(seen.values())} units")
        lines.append(f"{'Closing Bal'.ljust(15)} | {closing_bal} units")
        lines.append("━" * 28)
    return "\n".join(lines)


@sales_bp.route("", methods=["GET"])
@require_auth
def list_sales(group_id):
    uid = get_jwt_identity()
    db = get_db()
    canonical_only = _is_true(request.args.get("canonical"))
    is_mem, _ = cached_is_member(group_id, uid, lambda: _check_member(db, group_id, uid))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    cache_name = "sales_canonical" if canonical_only else "sales"
    cached_payload = get_cached_group_payload(cache_name, group_id)
    if cached_payload is not None:
        return jsonify(cached_payload)

    sale_map = {}

    # ── Source 1: Android path — BUSINESS_DATA/{groupId}/sales ───────────────
    try:
        for d in _bd_sales_ref(db, group_id).get():
            sale_map[d.id] = sale_to_dict(d.id, d.to_dict())
    except Exception:
        pass

    # ── Source 2: new backend — flat CASH_SALE / CREDIT_SALE collections ──────
    try:
        cash_docs   = db.collection(C.CASH_SALE  ).where("group_id", "==", group_id).get()
        credit_docs = db.collection(C.CREDIT_SALE).where("group_id", "==", group_id).get()
        for d in list(cash_docs) + list(credit_docs):
            if d.id not in sale_map:
                sale_map[d.id] = sale_to_dict(d.id, d.to_dict())
    except Exception:
        pass

    # ── Source 3: original project — nested subcollection structure ───────────
    if not canonical_only:
        for coll_name, is_credit_flag in [(C.CASH_SALE, False), (C.CREDIT_SALE, True)]:
            try:
                grp_ref   = db.collection(coll_name).document(group_id)
                prod_refs = list(grp_ref.collection("sales").list_documents())
                for prod_ref in prod_refs:
                    for entry_doc in prod_ref.collection("entries").stream():
                        if entry_doc.id not in sale_map:
                            d = entry_doc.to_dict() or {}
                            d.setdefault("is_credit", is_credit_flag)
                            sale_map[entry_doc.id] = sale_to_dict(entry_doc.id, d)
            except Exception as e:
                logging.exception("list_sales Source 3 error (%s %s): %s", coll_name, group_id, e)

    sales = sorted(sale_map.values(), key=lambda s: s["date"], reverse=True)
    payload = {"sales": sales}
    set_cached_group_payload(cache_name, group_id, payload)
    return jsonify(payload)


@sales_bp.route("/<sale_id>", methods=["GET"])
@require_auth
def get_sale(group_id, sale_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = cached_is_member(group_id, uid, lambda: _check_member(db, group_id, uid))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    # Try Android path first
    bd_doc = _bd_sales_ref(db, group_id).document(sale_id).get()
    if bd_doc.exists:
        return jsonify({"sale": sale_to_dict(bd_doc.id, bd_doc.to_dict())})

    # Try flat collections
    for coll in [C.CREDIT_SALE, C.CASH_SALE]:
        doc = db.collection(coll).document(sale_id).get()
        if doc.exists and doc.to_dict().get("group_id") == group_id:
            return jsonify({"sale": sale_to_dict(doc.id, doc.to_dict())})

    return jsonify({"error": "Sale not found"}), 404


@sales_bp.route("/<sale_id>", methods=["PUT"])
@require_auth
def update_sale(group_id, sale_id):
    """Update a sale record — e.g. mark as paid, change person name, etc."""
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = cached_is_member(group_id, uid, lambda: _check_member(db, group_id, uid))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    data = request.get_json() or {}
    updates = {}
    if "paymentStatus" in data or "payment_status" in data:
        val = data.get("paymentStatus", data.get("payment_status", True))
        updates["payment_status"] = bool(val)
        updates["paymentStatus"]  = bool(val)
    if "personName" in data or "person_name" in data:
        updates["person_name"] = data.get("personName") or data.get("person_name", "")
        updates["personName"]  = updates["person_name"]
    if "customerId" in data or "customer_id" in data:
        updates["customer_id"] = data.get("customerId") or data.get("customer_id", "")
        updates["customerId"]  = updates["customer_id"]
    if "notes" in data:
        updates["notes"] = data["notes"]

    if not updates:
        return jsonify({"error": "No updatable fields provided"}), 400

    # Try Android path first
    bd_ref = _bd_sales_ref(db, group_id).document(sale_id)
    bd_doc = bd_ref.get()
    if bd_doc.exists:
        bd_ref.update(updates)
        _invalidate_sales_caches(group_id)
        updated = bd_ref.get()
        return jsonify({"sale": sale_to_dict(updated.id, updated.to_dict())})

    # Try flat collections
    for coll in [C.CREDIT_SALE, C.CASH_SALE]:
        doc = db.collection(coll).document(sale_id).get()
        if doc.exists and doc.to_dict().get("group_id") == group_id:
            doc.reference.update(updates)
            _invalidate_sales_caches(group_id)
            updated = doc.reference.get()
            return jsonify({"sale": sale_to_dict(updated.id, updated.to_dict())})

    return jsonify({"error": "Sale not found"}), 404


@sales_bp.route("", methods=["POST"])
@require_auth
def create_sale(group_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, admin_id = cached_is_member(group_id, uid, lambda: _check_member(db, group_id, uid))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    data = request.get_json() or {}
    items = data.get("items", [])
    if not items:
        return jsonify({"error": "items are required"}), 400

    admin_id = admin_id or uid
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    is_credit = data.get("isCredit", False)
    payment_method = data.get("paymentMethod", "cash")
    customer_id = data.get("customerId", "")
    person_name = data.get("personName", "Walk-in Customer")
    sale_type = "credit" if is_credit else "cash"

    sale_collection = C.CREDIT_SALE if is_credit else C.CASH_SALE

    created_sales = []
    stock_out_ids = []
    total = 0.0
    batch = db.batch()

    for item in items:
        product_id   = item.get("productId",   "")
        product_name = item.get("productName", "")
        unit_price   = float(item.get("unitPrice",  0))
        buying_price = float(item.get("costPrice",  0))
        quantity     = int  (item.get("quantity",   1))
        line_total   = unit_price * quantity
        total += line_total

        sale_id = str(uuid.uuid4())

        sale_data = {
            "group_id":       group_id,
            "product_id":     product_id,
            "product_name":   product_name,
            "unit_price":     unit_price,
            "buying_price":   buying_price,
            "quantity":       quantity,
            "person_name":    person_name,
            "customer_id":    customer_id,
            "payment_status": not is_credit,
            "is_credit":      is_credit,
            "payment_method": payment_method,
            "sale_type":      sale_type,
            "created_by":     uid,
            "date":           now,
        }

        # ── Primary: Android path BUSINESS_DATA/{groupId}/sales/{saleId} ─────
        batch.set(_bd_sales_ref(db, group_id).document(sale_id), sale_data)

        # ── Legacy: flat collection ───────────────────────────────────────────
        batch.set(db.collection(sale_collection).document(sale_id), sale_data)

        # ── Legacy: original nested format ────────────────────────────────────
        nested_entry = {
            "id":            sale_id,
            "product_id":    product_id,
            "unit_price":    unit_price,
            "date":          now,
            "quantity":      quantity,
            "name":          product_name,
            "personName":    person_name,
            "customerId":    customer_id,
            "paymentStatus": not is_credit,
        }
        nested_ref = (
            db.collection(sale_collection)
            .document(group_id)
            .collection("sales")
            .document(product_name or product_id)
            .collection("entries")
            .document(sale_id)
        )
        batch.set(nested_ref, nested_entry)

        created_sales.append(sale_to_dict(sale_id, sale_data))

        # Record stock-out movement in Android path
        stock_out_id = str(uuid.uuid4())
        stock_out_ids.append(stock_out_id)
        movement_data = {
            "group_id":       group_id,
            "product_id":     product_id,
            "name":           product_name,
            "unit_price":     unit_price,
            "buying_price":   buying_price,
            "measuring_unit": item.get("measuringUnit", "pcs"),
            "quantity":       quantity,
            "movementType":   "out",
            "sale_id":        sale_id,
            "date":           now,
            "id":             stock_out_id,
        }
        # Android path
        batch.set(
            _bd_stock_movements_ref(db, group_id).document(stock_out_id),
            movement_data,
        )
        # Legacy flat STOCK_OUT
        batch.set(db.collection(C.STOCK_OUT).document(stock_out_id), {
            "group_id":       group_id,
            "product_id":     product_id,
            "name":           product_name,
            "unit_price":     unit_price,
            "buying_price":   buying_price,
            "measuring_unit": item.get("measuringUnit", "pcs"),
            "quantity":       quantity,
            "date":           now,
            "id":             stock_out_id,
        })

    # Build sale description
    if len(items) == 1:
        i0 = items[0]
        description = (
            f"Sold {i0.get('quantity', 1)}x {i0.get('productName', '')} "
            f"at {i0.get('unitPrice', 0)} each"
        )
    else:
        description = f"Multiple products sold {'on credit' if is_credit else 'as cash'}"

    # Record as income in expenses (flat format)
    expense_id = str(uuid.uuid4())
    batch.set(db.collection(C.EXPENSES).document(expense_id), {
        "group_id":   group_id,
        "admin_id":   admin_id,
        "name":       description,
        "price":      total,
        "is_expense": False,
        "category":   "Sales",
        "created_by": uid,
        "timestamp":  now,
        "created_at": datetime.now(timezone.utc).isoformat(),
    })

    # Update customer debt for credit sales
    if is_credit and customer_id:
        # Try Android customers path first
        bd_cust_ref = (
            db.collection(C.BUSINESS_DATA)
            .document(group_id)
            .collection(C.BD_CUSTOMERS)
            .document(customer_id)
        )
        bd_cust_doc = bd_cust_ref.get()
        if bd_cust_doc.exists:
            cd = bd_cust_doc.to_dict()
            old_balance      = float(cd.get("balance",     0.0) or 0.0)
            old_total_credit = float(cd.get("totalCredit", 0.0) or 0.0)
            batch.update(bd_cust_ref, {
                "balance":     old_balance      + total,
                "totalDebt":   old_balance      + total,
                "totalCredit": old_total_credit + total,
            })
        else:
            cust_doc = db.collection(C.CUSTOMERS).document(customer_id).get()
            if cust_doc.exists and cust_doc.to_dict().get("group_id") == group_id:
                cd = cust_doc.to_dict()
                old_balance      = float(cd.get("balance",     0.0) or 0.0)
                old_total_credit = float(cd.get("totalCredit", 0.0) or 0.0)
                batch.update(cust_doc.reference, {
                    "balance":     old_balance      + total,
                    "totalDebt":   old_balance      + total,
                    "totalCredit": old_total_credit + total,
                })
            else:
                try:
                    orig_cust_ref = (
                        db.collection(C.CUSTOMERS)
                        .document(group_id)
                        .collection("customers")
                        .document(customer_id)
                    )
                    orig_cust_doc = orig_cust_ref.get()
                    if orig_cust_doc.exists:
                        cd2 = orig_cust_doc.to_dict() or {}
                        old_bal = float(cd2.get("balance") or cd2.get("totalDebt") or 0.0)
                        old_tc  = float(cd2.get("totalCredit", 0.0) or 0.0)
                        batch.update(orig_cust_ref, {
                            "balance":     old_bal + total,
                            "totalDebt":   old_bal + total,
                            "totalCredit": old_tc  + total,
                        })
                except Exception:
                    pass

    batch.commit()

    _invalidate_sales_caches(group_id)
    invalidate_group_payload("customers", group_id)
    invalidate_group_payload("customers_canonical", group_id)
    invalidate_group_payload("stock_out", group_id)
    invalidate_group_payload("stock_out_canonical", group_id)
    invalidate_group_payload("income", group_id)
    invalidate_group_payload("income_canonical", group_id)
    invalidate_report("stock", group_id)
    invalidate_report("stock_canonical", group_id)

    if not is_credit and payment_method == "mpesa":
        _increment_group_account_balance(db, group_id, total)

    # Fire-and-forget: post sale notification + update chat previews
    try:
        db.collection(C.EXPENSES).document(admin_id).set({
            expense_id: {
                "id":        expense_id,
                "timestamp": now,
                "isExpense": False,
                "chatID":    group_id,
                "name":      description,
                "price":     total,
                "createdBy": uid,
            }
        }, merge=True)
    except Exception:
        pass

    # Legacy stock-out format writes
    for item, stock_out_id in zip(items, stock_out_ids):
        prod_name_so = item.get("productName", "")
        if not prod_name_so:
            continue
        so_entry = {
            "id":             stock_out_id,
            "product_id":     item.get("productId", ""),
            "name":           prod_name_so,
            "measuring_unit": item.get("measuringUnit", "pcs"),
            "buying_price":   float(item.get("costPrice", 0) or 0),
            "unit_price":     float(item.get("unitPrice",  0) or 0),
            "date":           now,
            "unit":           1,
            "quantity":       int(item.get("quantity", 1) or 1),
        }
        try:
            db.collection(C.STOCK_OUT).document(prod_name_so).set(
                {stock_out_id: so_entry}, merge=True
            )
        except Exception:
            pass
        try:
            db.collection(C.STOCK_OUT).document(group_id).set(
                {stock_out_id: so_entry}, merge=True
            )
        except Exception:
            pass

    _post_sale_notification(db, uid, group_id, description, total, now)

    return jsonify({"sales": created_sales}), 201


@sales_bp.route("/multi", methods=["POST"])
@require_auth
def create_multi_sale(group_id):
    """
    Multi-product sale in a single atomic batch.

    Body:
      {
        "items": [{"productId", "productName", "quantity", "unitPrice", "buyingPrice", "measuringUnit"?}],
        "saleType": "cash"|"credit",
        "customerId"?: str,
        "personName"?: str,
        "date"?: int (ms epoch)
      }

    For each item:
      - Decrement product.available_stock (Android + flat paths)
      - Write sale record to BUSINESS_DATA/{groupId}/sales/{saleId} and legacy paths
      - Write stock-out movement to BUSINESS_DATA/{groupId}/stock_movements/{id}

    Returns {"sales": [...], "total": float}
    """
    uid = get_jwt_identity()
    db = get_db()
    is_mem, admin_id = cached_is_member(group_id, uid, lambda: _check_member(db, group_id, uid))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    data = request.get_json() or {}
    items = data.get("items", [])
    if not items:
        return jsonify({"error": "items are required"}), 400

    sale_type   = str(data.get("saleType", "cash")).lower()
    is_credit   = sale_type == "credit"
    customer_id = data.get("customerId", "")
    person_name = data.get("personName", "Walk-in Customer")
    admin_id    = admin_id or uid
    now         = int(data.get("date") or datetime.now(timezone.utc).timestamp() * 1000)

    sale_collection = C.CREDIT_SALE if is_credit else C.CASH_SALE

    # Pre-fetch product docs so we can decrement stock atomically
    product_refs = {}
    for item in items:
        pid = item.get("productId", "")
        if pid and pid not in product_refs:
            # Prefer Android path; fall back to flat collection
            bd_ref = (
                db.collection(C.BUSINESS_DATA)
                .document(group_id)
                .collection(C.BD_PRODUCTS)
                .document(pid)
            )
            bd_doc = bd_ref.get()
            if bd_doc.exists:
                product_refs[pid] = (bd_ref, bd_doc)
            else:
                flat_doc = db.collection(C.PRODUCTS).document(pid).get()
                if flat_doc.exists and flat_doc.to_dict().get("group_id") == group_id:
                    product_refs[pid] = (flat_doc.reference, flat_doc)
                else:
                    product_refs[pid] = (None, None)

    created_sales = []
    total = 0.0
    batch = db.batch()

    for item in items:
        product_id   = item.get("productId",   "")
        product_name = item.get("productName", "")
        unit_price   = float(item.get("unitPrice",   0))
        buying_price = float(item.get("buyingPrice", 0))
        quantity     = int  (item.get("quantity",    1))
        meas_unit    = item.get("measuringUnit", "pcs")
        line_total   = unit_price * quantity
        total += line_total

        sale_id = str(uuid.uuid4())
        sale_data = {
            "group_id":       group_id,
            "product_id":     product_id,
            "product_name":   product_name,
            "unit_price":     unit_price,
            "buying_price":   buying_price,
            "quantity":       quantity,
            "person_name":    person_name,
            "customer_id":    customer_id,
            "payment_status": not is_credit,
            "is_credit":      is_credit,
            "sale_type":      sale_type,
            "created_by":     uid,
            "date":           now,
        }

        # Primary Android path
        batch.set(_bd_sales_ref(db, group_id).document(sale_id), sale_data)
        # Legacy flat
        batch.set(db.collection(sale_collection).document(sale_id), sale_data)

        created_sales.append(sale_to_dict(sale_id, sale_data))

        # Decrement available_stock
        ref, doc = product_refs.get(product_id, (None, None))
        if ref and doc:
            current = int((doc.to_dict() or {}).get("available_stock", 0) or 0)
            batch.update(ref, {"available_stock": max(0, current - quantity)})

        # Stock-out movement (Android path)
        movement_id = str(uuid.uuid4())
        batch.set(
            _bd_stock_movements_ref(db, group_id).document(movement_id),
            {
                "group_id":       group_id,
                "product_id":     product_id,
                "name":           product_name,
                "unit_price":     unit_price,
                "buying_price":   buying_price,
                "measuring_unit": meas_unit,
                "quantity":       quantity,
                "movementType":   "out",
                "sale_id":        sale_id,
                "date":           now,
                "id":             movement_id,
            },
        )
        # Legacy flat STOCK_OUT
        batch.set(db.collection(C.STOCK_OUT).document(movement_id), {
            "group_id":       group_id,
            "product_id":     product_id,
            "name":           product_name,
            "unit_price":     unit_price,
            "buying_price":   buying_price,
            "measuring_unit": meas_unit,
            "quantity":       quantity,
            "date":           now,
            "id":             movement_id,
        })

    # Description
    if len(items) == 1:
        i0 = items[0]
        description = f"Sold {i0.get('quantity', 1)}x {i0.get('productName', '')} at {i0.get('unitPrice', 0)} each"
    else:
        description = f"Multi-product sale {'on credit' if is_credit else 'cash'} — {len(items)} items"

    # Income record
    expense_id = str(uuid.uuid4())
    batch.set(db.collection(C.EXPENSES).document(expense_id), {
        "group_id":   group_id,
        "admin_id":   admin_id,
        "name":       description,
        "price":      total,
        "is_expense": False,
        "category":   "Sales",
        "created_by": uid,
        "timestamp":  now,
        "created_at": datetime.now(timezone.utc).isoformat(),
    })

    # Update customer debt
    if is_credit and customer_id:
        bd_cust_ref = (
            db.collection(C.BUSINESS_DATA)
            .document(group_id)
            .collection(C.BD_CUSTOMERS)
            .document(customer_id)
        )
        bd_cust_doc = bd_cust_ref.get()
        if bd_cust_doc.exists:
            cd = bd_cust_doc.to_dict()
            old_bal = float(cd.get("balance", 0.0) or 0.0)
            old_tc  = float(cd.get("totalCredit", 0.0) or 0.0)
            batch.update(bd_cust_ref, {
                "balance":     old_bal + total,
                "totalDebt":   old_bal + total,
                "totalCredit": old_tc  + total,
            })
        else:
            flat_cust = db.collection(C.CUSTOMERS).document(customer_id).get()
            if flat_cust.exists and flat_cust.to_dict().get("group_id") == group_id:
                cd = flat_cust.to_dict()
                old_bal = float(cd.get("balance", 0.0) or 0.0)
                old_tc  = float(cd.get("totalCredit", 0.0) or 0.0)
                batch.update(flat_cust.reference, {
                    "balance":     old_bal + total,
                    "totalDebt":   old_bal + total,
                    "totalCredit": old_tc  + total,
                })

    batch.commit()

    _invalidate_sales_caches(group_id)
    invalidate_group_payload("customers", group_id)
    invalidate_group_payload("customers_canonical", group_id)
    invalidate_group_payload("stock_out", group_id)
    invalidate_group_payload("stock_out_canonical", group_id)
    invalidate_group_payload("income", group_id)
    invalidate_group_payload("income_canonical", group_id)
    invalidate_report("stock", group_id)
    invalidate_report("stock_canonical", group_id)
    invalidate_products(group_id)
    invalidate_group_payload("products_canonical", group_id)

    _post_sale_notification(db, uid, group_id, description, total, now)

    return jsonify({"sales": created_sales, "total": total}), 201


@sales_bp.route("/<sale_id>/mark-paid", methods=["PUT"])
@require_auth
def mark_paid(group_id, sale_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = cached_is_member(group_id, uid, lambda: _check_member(db, group_id, uid))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    # ── Try Android path first ────────────────────────────────────────────────
    bd_ref = _bd_sales_ref(db, group_id).document(sale_id)
    bd_doc = bd_ref.get()
    if bd_doc.exists:
        bd_ref.update({"payment_status": True, "paymentStatus": True})
        _invalidate_sales_caches(group_id)
        updated = bd_ref.get()
        return jsonify({"sale": sale_to_dict(updated.id, updated.to_dict())})

    # ── Flat collection ───────────────────────────────────────────────────────
    doc = db.collection(C.CREDIT_SALE).document(sale_id).get()
    if not doc.exists:
        doc = db.collection(C.CASH_SALE).document(sale_id).get()
    if doc.exists and doc.to_dict().get("group_id") == group_id:
        doc.reference.update({"payment_status": True, "paymentStatus": True})
        _invalidate_sales_caches(group_id)
        updated = doc.reference.get()
        return jsonify({"sale": sale_to_dict(updated.id, updated.to_dict())})

    # ── Original project — nested CREDIT_SALE subcollection ──────────────────
    try:
        grp_ref   = db.collection(C.CREDIT_SALE).document(group_id)
        prod_refs = list(grp_ref.collection("sales").list_documents())
        for prod_ref in prod_refs:
            entry_ref = prod_ref.collection("entries").document(sale_id)
            entry_doc = entry_ref.get()
            if entry_doc.exists:
                entry_ref.update({"paymentStatus": True})
                _invalidate_sales_caches(group_id)
                d = entry_doc.to_dict() or {}
                d["paymentStatus"] = True
                d.setdefault("is_credit", True)
                return jsonify({"sale": sale_to_dict(sale_id, d)})
    except Exception as e:
        logging.exception("mark_paid Source 3 error (%s %s): %s", group_id, sale_id, e)

    return jsonify({"error": "Sale not found"}), 404
