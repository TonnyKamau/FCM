from flask import Blueprint, request, jsonify
from firebase_utils import get_db
from models import stock_in_to_dict, stock_out_to_dict
from auth_utils import require_auth, get_jwt_identity
import db_constants as C
import uuid
from datetime import datetime, timezone
from cache_utils import cached_is_member, get_cached_group_payload, set_cached_group_payload, invalidate_group_payload, invalidate_report

stock_bp = Blueprint("stock", __name__, url_prefix="/groups/<group_id>/stock")


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


@stock_bp.route("/in", methods=["GET"])
@require_auth
def list_stock_in(group_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = cached_is_member(group_id, uid, lambda: (_is_member(db, group_id, uid), None))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    cached_payload = get_cached_group_payload("stock_in", group_id)
    if cached_payload is not None:
        return jsonify(cached_payload)

    # ── Source 1: new backend — flat STOCK collection with group_id field ──────
    docs = db.collection(C.STOCK).where("group_id", "==", group_id).get()
    entry_map = {d.id: stock_in_to_dict(d.id, d.to_dict()) for d in docs}

    # ── Source 2: original Windows Flutter — STOCK/{groupId} map document ───────
    try:
        orig_stock_doc = db.collection(C.STOCK).document(group_id).get()
        if orig_stock_doc.exists:
            for key, val in (orig_stock_doc.to_dict() or {}).items():
                if not isinstance(val, dict):
                    continue
                if "id" in val and "name" in val:
                    if key not in entry_map:
                        entry_map[key] = stock_in_to_dict(key, val)
                else:
                    for stock_id, stock_entry in val.items():
                        if isinstance(stock_entry, dict) and stock_id not in entry_map:
                            entry_map[stock_id] = stock_in_to_dict(stock_id, stock_entry)
    except Exception as e:
        import logging
        logging.exception("list_stock_in Source 2 error (%s): %s", group_id, e)

    # ── Source 3: Android app — STOCK/{groupId}/{productName}/{stockId} ──────────
    try:
        grp_ref = db.collection(C.STOCK).document(group_id)
        for sub_coll in grp_ref.collections():
            for d in sub_coll.stream():
                if d.id not in entry_map:
                    entry_map[d.id] = stock_in_to_dict(d.id, d.to_dict() or {})
    except Exception as e:
        import logging
        logging.exception("list_stock_in Source 3 (Android) error (%s): %s", group_id, e)

    entries = sorted(entry_map.values(), key=lambda e: e["date"], reverse=True)
    payload = {"stockIn": entries}
    set_cached_group_payload("stock_in", group_id, payload)
    return jsonify(payload)


@stock_bp.route("/in", methods=["POST"])
@require_auth
def add_stock_in(group_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = cached_is_member(group_id, uid, lambda: (_is_member(db, group_id, uid), None))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    data = request.get_json() or {}
    product_id = data.get("productId", "")
    product_name = data.get("productName", "")
    quantity = int(data.get("quantity", 0))
    if not product_id or quantity <= 0:
        return jsonify({"error": "productId and quantity are required"}), 400

    buying_price = float(data.get("buyingPrice", 0))
    unit_price = float(data.get("unitPrice", 0))
    measuring_unit = data.get("measuringUnit", "pcs")
    now = int(datetime.now(timezone.utc).timestamp() * 1000)

    entry_id = str(uuid.uuid4())
    entry_data = {
        "group_id": group_id,
        "product_id": product_id,
        "name": product_name,
        "measuring_unit": measuring_unit,
        "buying_price": buying_price,
        "unit_price": unit_price,
        "quantity": quantity,
        "sold_amount": 0.0,
        "total_available": quantity,
        "date": now,
    }
    db.collection(C.STOCK).document(entry_id).set(entry_data)

    # ── Write to Windows Flutter format: STOCK/{groupId} map document ──────────
    orig_entry = {
        "id": entry_id, "product_id": product_id, "name": product_name,
        "measuring_unit": measuring_unit, "buying_price": buying_price,
        "unit_price": unit_price, "quantity": quantity, "unit": 1,
        "sold_amount": 0.0, "total_available": quantity, "date": now,
    }
    try:
        db.collection(C.STOCK).document(group_id).set(
            {product_name: {entry_id: orig_entry}}, merge=True
        )
    except Exception:
        pass

    # ── Write to Android format: STOCK/{groupId}/{sanitizedName}/{entryId} ──────
    try:
        san_name = (product_name or product_id).replace("/", "_").replace("\\", "_").replace(".", "_")
        android_entry = {
            "id": entry_id, "product_id": product_id, "name": product_name,
            "measuring_unit": measuring_unit, "buying_price": buying_price,
            "unit_price": unit_price, "quantity": quantity, "unit": 1, "date": now,
        }
        db.collection(C.STOCK).document(group_id).collection(san_name).document(entry_id).set(android_entry)
    except Exception:
        pass

    # Update product stock and prices — new backend format (flat PRODUCTS)
    stock_updated = False
    product_doc = db.collection(C.PRODUCTS).document(product_id).get()
    if product_doc.exists and product_doc.to_dict().get("group_id") == group_id:
        current_stock = int(product_doc.to_dict().get("available_stock", 0) or 0)
        product_doc.reference.update({
            "available_stock": current_stock + quantity,
            "buying_price": buying_price,
            "unit_price": unit_price,
        })
        stock_updated = True

    if not stock_updated:
        try:
            orig_prod_ref = db.collection(C.PRODUCTS).document(group_id)
            orig_prod_doc = orig_prod_ref.get()
            if orig_prod_doc.exists:
                prod_data = (orig_prod_doc.to_dict() or {}).get(product_id)
                if isinstance(prod_data, dict):
                    cur_stk = int(
                        prod_data.get("available_stock") or
                        prod_data.get("availableStock") or 0
                    )
                    orig_prod_ref.update({
                        f"{product_id}.available_stock": cur_stk + quantity,
                        f"{product_id}.buying_price":    buying_price,
                        f"{product_id}.unit_price":      unit_price,
                    })
        except Exception:
            pass

    invalidate_group_payload("stock_in", group_id)
    invalidate_group_payload("products", group_id)
    invalidate_report("stock", group_id)
    return jsonify({"stockIn": stock_in_to_dict(entry_id, entry_data)}), 201


@stock_bp.route("/out", methods=["GET"])
@require_auth
def list_stock_out(group_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = cached_is_member(group_id, uid, lambda: (_is_member(db, group_id, uid), None))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    cached_payload = get_cached_group_payload("stock_out", group_id)
    if cached_payload is not None:
        return jsonify(cached_payload)

    # ── Source 1: new backend — flat STOCK_OUT collection ────────────────────
    docs = db.collection(C.STOCK_OUT).where("group_id", "==", group_id).get()
    entry_map = {d.id: stock_out_to_dict(d.id, d.to_dict()) for d in docs}

    # ── Source 2: original project — STOCK_OUT/{productName} map documents ──────
    import logging
    so2_names: set = set()
    try:
        for pd in db.collection(C.PRODUCTS).where("group_id", "==", group_id).get():
            n = (pd.to_dict() or {}).get("name", "")
            if n:
                so2_names.add(n)
    except Exception:
        pass
    try:
        opd = db.collection(C.PRODUCTS).document(group_id).get()
        if opd.exists:
            for opv in (opd.to_dict() or {}).values():
                if isinstance(opv, dict):
                    n = opv.get("name", "")
                    if n:
                        so2_names.add(n)
    except Exception:
        pass
    for pn in so2_names:
        try:
            so2 = db.collection(C.STOCK_OUT).document(pn).get()
            if so2.exists:
                for stock_id, val in (so2.to_dict() or {}).items():
                    if isinstance(val, dict) and stock_id not in entry_map:
                        entry_map[stock_id] = stock_out_to_dict(stock_id, val)
        except Exception as e:
            logging.exception(
                "list_stock_out Source 2 error (%s, %s): %s", group_id, pn, e
            )

    # ── Source 3: Android app — STOCK_OUT/{groupId} map document ─────────────────
    try:
        android_so = db.collection(C.STOCK_OUT).document(group_id).get()
        if android_so.exists:
            for out_id, val in (android_so.to_dict() or {}).items():
                if isinstance(val, dict) and out_id not in entry_map:
                    entry_map[out_id] = stock_out_to_dict(out_id, val)
    except Exception as e:
        logging.exception("list_stock_out Source 3 (Android) error (%s): %s", group_id, e)

    entries = sorted(entry_map.values(), key=lambda e: e["date"], reverse=True)
    payload = {"stockOut": entries}
    set_cached_group_payload("stock_out", group_id, payload)
    return jsonify(payload)
