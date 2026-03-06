from flask import Blueprint, request, jsonify
from firebase_utils import get_db
from models import sale_to_dict
from auth_utils import require_auth, get_jwt_identity
import db_constants as C
import logging
import uuid
from datetime import datetime, timezone

sales_bp = Blueprint("sales", __name__, url_prefix="/groups/<group_id>/sales")


def _check_member(db, group_id, uid):
    """Returns (is_member: bool, admin_id: str|None)."""
    # ── New backend: GroupAccounts + GroupMembers ──────────────────────────────
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
    # ── Original project: CHATS/{uid} map contains the group_id key ───────────
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
        # Count today's units sold — deduplicate flat vs nested sources by sale_id
        seen: dict = {}  # {sale_id: quantity}
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
        for coll in [C.CASH_SALE, C.CREDIT_SALE]:
            try:
                for d in (
                    db.collection(coll)
                    .document(group_id)
                    .collection("sales")
                    .document(prod_name)
                    .collection("entries")
                    .get()
                ):
                    dd = d.to_dict() or {}
                    if (dd.get("date", 0) or 0) >= start_of_day and d.id not in seen:
                        seen[d.id] = int(dd.get("quantity", 0) or 0)
            except Exception as e:
                logging.exception("stock_card nested query (%s): %s", coll, e)
        total_sold_today = sum(seen.values())
        # Closing balance from PRODUCTS (flat or original map format)
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
        lines.append(f"{'Sold Today'.ljust(15)} | {total_sold_today} units")
        lines.append(f"{'Closing Bal'.ljust(15)} | {closing_bal} units")
        lines.append("━" * 28)
    return "\n".join(lines)


@sales_bp.route("", methods=["GET"])
@require_auth
def list_sales(group_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = _check_member(db, group_id, uid)
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    # ── Source 1: new backend — flat CASH_SALE / CREDIT_SALE collections ──────
    cash_docs   = db.collection(C.CASH_SALE  ).where("group_id", "==", group_id).get()
    credit_docs = db.collection(C.CREDIT_SALE).where("group_id", "==", group_id).get()
    sale_map = {}
    for d in list(cash_docs) + list(credit_docs):
        sale_map[d.id] = sale_to_dict(d.id, d.to_dict())

    # ── Source 2: original project — nested subcollection structure ───────────
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
            logging.exception("list_sales Source 2 error (%s %s): %s", coll_name, group_id, e)

    sales = sorted(sale_map.values(), key=lambda s: s["date"], reverse=True)
    return jsonify({"sales": sales})


@sales_bp.route("", methods=["POST"])
@require_auth
def create_sale(group_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, admin_id = _check_member(db, group_id, uid)
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    data = request.get_json() or {}
    items = data.get("items", [])
    if not items:
        return jsonify({"error": "items are required"}), 400

    admin_id = admin_id or uid
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    is_credit = data.get("isCredit", False)
    customer_id = data.get("customerId", "")
    person_name = data.get("personName", "Walk-in Customer")

    sale_collection = C.CREDIT_SALE if is_credit else C.CASH_SALE

    created_sales = []
    stock_out_ids = []   # parallel list — one stock_out_id per item, used below
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

        # ── Flat collection (new backend format) ──────────────────────────────
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
            "created_by":     uid,
            "date":           now,
        }
        batch.set(db.collection(sale_collection).document(sale_id), sale_data)

        # ── Original nested format ──────────────────────────────────────────
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

        # ── Stock deduction is handled by the Flutter app via PUT /adjust-stock.
        # Do NOT deduct here to avoid double-deducting.

        # Record stock out — use a single shared ID so all three write formats
        # deduplicate to one entry when list_stock_out() reads them back.
        stock_out_id = str(uuid.uuid4())
        stock_out_ids.append(stock_out_id)
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
        description = (
            f"Multiple products sold {'on credit' if is_credit else 'as cash'}"
        )

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
        cust_doc = db.collection(C.CUSTOMERS).document(customer_id).get()
        if cust_doc.exists and cust_doc.to_dict().get("group_id") == group_id:
            cd = cust_doc.to_dict()
            old_balance     = float(cd.get("balance",     0.0) or 0.0)
            old_total_credit = float(cd.get("totalCredit", 0.0) or 0.0)
            batch.update(cust_doc.reference, {
                "balance":      old_balance      + total,
                "totalDebt":    old_balance      + total,
                "totalCredit":  old_total_credit + total,
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
                        "balance":      old_bal + total,
                        "totalDebt":    old_bal + total,
                        "totalCredit":  old_tc  + total,
                    })
            except Exception:
                pass

    batch.commit()

    # ── After commit: post sale notification + update CHATS (fire-and-forget) ──
    try:
        user_doc = db.collection(C.USERS).document(uid).get()
        sender_name = user_doc.to_dict().get("name", "User") if user_doc.exists else "User"

        msg_id   = str(uuid.uuid4())
        msg_text = description

        # Write to original MESSAGES/{chatId} map document
        db.collection(C.MESSAGES).document(group_id).set({
            msg_id: {
                "id":            msg_id,
                "senderID":      uid,
                "senderName":    sender_name,
                "receiverID":    "",
                "receiverName":  "",
                "chatID":        group_id,
                "isGroup":       True,
                "isMoneyShared": False,
                "isImageShared": False,
                "isPoll":        False,
                "isLoanRequest": False,
                "money":         "",
                "image":         "",
                "caption":       "",
                "message":       msg_text,
                "timestamp":     now,
            }
        }, merge=True)

        # Also write to new flat MESSAGES collection
        db.collection(C.MESSAGES).document(msg_id).set({
            "group_id":        group_id,
            "sender_id":       uid,
            "sender_name":     sender_name,
            "message":         msg_text,
            "is_group":        True,
            "is_money_shared": False,
            "is_image_shared": False,
            "is_poll":         False,
            "is_loan_request": False,
            "money":           "",
            "image":           "",
            "caption":         "",
            "timestamp":       now,
        })

        # Also write income to original EXPENSES/{adminId} map format
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

        # Update CHATS/{memberId} for all group members
        last_msg = f"{sender_name}: {msg_text}"
        chat_update = {
            f"{group_id}.timestamp":    now,
            f"{group_id}.lastMessage":  last_msg,
            f"{group_id}.isMoneyShared": False,
            f"{group_id}.isImageShared": False,
            f"{group_id}.isGroup":       True,
            f"{group_id}.senderName":    sender_name,
        }
        db.collection(C.CHATS).document(uid).set(chat_update, merge=True)

        gm_docs = (
            db.collection(C.GROUP_MEMBERS)
            .where("group_id", "==", group_id)
            .get()
        )
        for gm in gm_docs:
            member_uid = gm.to_dict().get("user_id", "")
            if member_uid and member_uid != uid:
                db.collection(C.CHATS).document(member_uid).set(
                    chat_update, merge=True
                )

        try:
            db.collection(C.GROUP_ACCOUNTS).document(group_id).update({
                "last_message": last_msg,
                "timestamp":    now,
            })
        except Exception:
            pass

        # ── Write STOCK_OUT to original formats ───────────────────────────────
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
            # Windows Flutter format: STOCK_OUT/{productName} → { stockId: {...} }
            try:
                db.collection(C.STOCK_OUT).document(prod_name_so).set(
                    {stock_out_id: so_entry}, merge=True
                )
            except Exception:
                pass
            # Android format: STOCK_OUT/{groupId} → { outId: {...} }
            try:
                db.collection(C.STOCK_OUT).document(group_id).set(
                    {stock_out_id: so_entry}, merge=True
                )
            except Exception:
                pass

        # ── Send stock card message to group chat ────────────────────────────
        try:
            stock_card_msg = _build_stock_card(db, group_id, items, now)
            sc_id  = str(uuid.uuid4())
            sc_ts  = now + 1000

            db.collection(C.MESSAGES).document(group_id).set({
                sc_id: {
                    "id":            sc_id,
                    "senderID":      uid,
                    "senderName":    "Stock Update",
                    "receiverID":    "",
                    "receiverName":  "",
                    "chatID":        group_id,
                    "isGroup":       True,
                    "isMoneyShared": False,
                    "isImageShared": False,
                    "isPoll":        False,
                    "isLoanRequest": False,
                    "money":         "",
                    "image":         "",
                    "caption":       "",
                    "message":       stock_card_msg,
                    "timestamp":     sc_ts,
                }
            }, merge=True)

            db.collection(C.MESSAGES).document(sc_id).set({
                "group_id":        group_id,
                "sender_id":       uid,
                "sender_name":     "Stock Update",
                "message":         stock_card_msg,
                "is_group":        True,
                "is_money_shared": False,
                "is_image_shared": False,
                "is_poll":         False,
                "is_loan_request": False,
                "money":           "",
                "image":           "",
                "caption":         "",
                "timestamp":       sc_ts,
            })

            sc_last = f"Stock Update: {stock_card_msg}"
            sc_chat_update = {
                f"{group_id}.timestamp":     sc_ts,
                f"{group_id}.lastMessage":   sc_last,
                f"{group_id}.isMoneyShared": False,
                f"{group_id}.isImageShared": False,
                f"{group_id}.isGroup":       True,
                f"{group_id}.senderName":    "Stock Update",
            }
            db.collection(C.CHATS).document(uid).set(sc_chat_update, merge=True)
            for gm in gm_docs:
                m_uid = gm.to_dict().get("user_id", "")
                if m_uid and m_uid != uid:
                    db.collection(C.CHATS).document(m_uid).set(
                        sc_chat_update, merge=True
                    )

            try:
                db.collection(C.GROUP_ACCOUNTS).document(group_id).update({
                    "last_message": sc_last,
                    "timestamp":    sc_ts,
                })
            except Exception:
                pass
        except Exception:
            pass  # stock card failure must not block

    except Exception:
        pass  # notification failure must never block the sale response

    return jsonify({"sales": created_sales}), 201


@sales_bp.route("/<sale_id>/mark-paid", methods=["PUT"])
@require_auth
def mark_paid(group_id, sale_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = _check_member(db, group_id, uid)
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    # ── Source 1: new backend flat collection ─────────────────────────────────
    doc = db.collection(C.CREDIT_SALE).document(sale_id).get()
    if not doc.exists:
        doc = db.collection(C.CASH_SALE).document(sale_id).get()
    if doc.exists and doc.to_dict().get("group_id") == group_id:
        doc.reference.update({"payment_status": True, "paymentStatus": True})
        updated = doc.reference.get()
        return jsonify({"sale": sale_to_dict(updated.id, updated.to_dict())})

    # ── Source 2: original project — nested CREDIT_SALE subcollection ─────────
    try:
        grp_ref   = db.collection(C.CREDIT_SALE).document(group_id)
        prod_refs = list(grp_ref.collection("sales").list_documents())
        for prod_ref in prod_refs:
            entry_ref = prod_ref.collection("entries").document(sale_id)
            entry_doc = entry_ref.get()
            if entry_doc.exists:
                entry_ref.update({"paymentStatus": True})
                d = entry_doc.to_dict() or {}
                d["paymentStatus"] = True
                d.setdefault("is_credit", True)
                return jsonify({"sale": sale_to_dict(sale_id, d)})
    except Exception as e:
        logging.exception("mark_paid Source 2 error (%s %s): %s", group_id, sale_id, e)

    return jsonify({"error": "Sale not found"}), 404
