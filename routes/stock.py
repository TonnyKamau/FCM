from flask import Blueprint, request, jsonify
from firebase_utils import get_db
from models import stock_in_to_dict, stock_out_to_dict
from auth_utils import require_auth, get_jwt_identity
import db_constants as C
import uuid
from datetime import datetime, timezone
from cache_utils import (
    cached_is_member,
    get_cached_group_payload,
    set_cached_group_payload,
    invalidate_group_payload,
    invalidate_report,
)
from routes.messages import post_group_event_message

stock_bp = Blueprint("stock", __name__, url_prefix="/groups/<group_id>/stock")


def _is_true(value):
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _is_member(db, group_id, uid):
    doc = db.collection(C.GROUP_ACCOUNTS).document(group_id).get()
    if doc.exists:
        if doc.to_dict().get("admin_id") == uid:
            return True
        gm = list(
            db.collection(C.GROUP_MEMBERS)
            .where("group_id", "==", group_id)
            .where("user_id", "==", uid)
            .limit(1)
            .get()
        )
        if gm:
            return True
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
    try:
        chats_doc = db.collection(C.CHATS).document(uid).get()
        if chats_doc.exists:
            chat_data = chats_doc.to_dict() or {}
            if group_id in chat_data and isinstance(chat_data[group_id], dict):
                return True
    except Exception:
        pass
    return False


def _bd_movements_ref(db, group_id):
    """BUSINESS_DATA/{groupId}/stock_movements subcollection."""
    return (
        db.collection(C.BUSINESS_DATA)
        .document(group_id)
        .collection(C.BD_STOCK_MOVEMENTS)
    )


def _bd_products_ref(db, group_id):
    """BUSINESS_DATA/{groupId}/products subcollection."""
    return (
        db.collection(C.BUSINESS_DATA)
        .document(group_id)
        .collection(C.BD_PRODUCTS)
    )


def _movement_to_dict(doc_id, d):
    """Normalise a stock movement document (in or out) to a consistent shape."""
    movement_type = d.get("movementType") or d.get("movement_type") or "in"
    return {
        "id":             doc_id,
        "product_id":     d.get("product_id", ""),
        "name":           d.get("name", ""),
        "measuring_unit": d.get("measuring_unit", "pcs"),
        "buying_price":   float(d.get("buying_price", 0) or 0),
        "unit_price":     float(d.get("unit_price", 0) or 0),
        "quantity":       int(d.get("quantity", 0) or 0),
        "movementType":   movement_type,
        "sale_id":        d.get("sale_id", ""),
        "date":           d.get("date", 0),
    }


@stock_bp.route("/in", methods=["GET"])
@require_auth
def list_stock_in(group_id):
    uid = get_jwt_identity()
    db = get_db()
    canonical_only = _is_true(request.args.get("canonical"))
    is_mem, _ = cached_is_member(
        group_id,
        uid,
        lambda: (_is_member(db, group_id, uid), None),
    )
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    cache_name = "stock_in_canonical" if canonical_only else "stock_in"
    cached_payload = get_cached_group_payload(cache_name, group_id)
    if cached_payload is not None:
        return jsonify(cached_payload)

    entry_map = {}

    # ── Source 1: Android path — BUSINESS_DATA/{groupId}/stock_movements (type=in) ──
    try:
        for d in _bd_movements_ref(db, group_id).where("movementType", "==", "in").get():
            entry_map[d.id] = stock_in_to_dict(d.id, d.to_dict())
    except Exception:
        pass

    # ── Source 2: flat STOCK collection ──────────────────────────────────────
    try:
        docs = db.collection(C.STOCK).where("group_id", "==", group_id).get()
        for d in docs:
            if d.id not in entry_map:
                entry_map[d.id] = stock_in_to_dict(d.id, d.to_dict())
    except Exception:
        pass

    if not canonical_only:
        # ── Source 3: original map document STOCK/{groupId} ──────────────────
        try:
            orig_stock_doc = db.collection(C.STOCK).document(group_id).get()
            if orig_stock_doc.exists:
                for key, val in (orig_stock_doc.to_dict() or {}).items():
                    if not isinstance(val, dict):
                        continue
                    if "id" in val and "name" in val:
                        entry_map.setdefault(key, stock_in_to_dict(key, val))
                    else:
                        for stock_id, stock_entry in val.items():
                            if isinstance(stock_entry, dict) and stock_id not in entry_map:
                                entry_map[stock_id] = stock_in_to_dict(stock_id, stock_entry)
        except Exception:
            pass

        # ── Source 4: STOCK/{groupId}/<subcollections> ────────────────────────
        try:
            grp_ref = db.collection(C.STOCK).document(group_id)
            for sub_coll in grp_ref.collections():
                for doc in sub_coll.stream():
                    if doc.id not in entry_map:
                        entry_map[doc.id] = stock_in_to_dict(doc.id, doc.to_dict() or {})
        except Exception:
            pass

    entries = sorted(entry_map.values(), key=lambda e: e["date"], reverse=True)
    payload = {"stockIn": entries}
    set_cached_group_payload(cache_name, group_id, payload)
    return jsonify(payload)


@stock_bp.route("/in", methods=["POST"])
@require_auth
def add_stock_in(group_id):
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = cached_is_member(
        group_id,
        uid,
        lambda: (_is_member(db, group_id, uid), None),
    )
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
        "group_id":       group_id,
        "groupId":        group_id,
        "id":             entry_id,
        "product_id":     product_id,
        "productId":      product_id,
        "name":           product_name,
        "measuring_unit": measuring_unit,
        "buying_price":   buying_price,
        "unit_price":     unit_price,
        "quantity":       quantity,
        "sold_amount":    0.0,
        "total_available": quantity,
        "movementType":   "in",
        "date":           now,
    }

    # ── Primary: Android path BUSINESS_DATA/{groupId}/stock_movements/{id} ───
    _bd_movements_ref(db, group_id).document(entry_id).set(entry_data)

    # ── Legacy flat STOCK collection ──────────────────────────────────────────
    try:
        db.collection(C.STOCK).document(entry_id).set(entry_data)
    except Exception:
        pass

    # ── Legacy map document STOCK/{groupId} ───────────────────────────────────
    orig_entry = {
        "id": entry_id,
        "product_id": product_id,
        "name": product_name,
        "measuring_unit": measuring_unit,
        "buying_price": buying_price,
        "unit_price": unit_price,
        "quantity": quantity,
        "unit": 1,
        "sold_amount": 0.0,
        "total_available": quantity,
        "date": now,
    }
    try:
        db.collection(C.STOCK).document(group_id).set(
            {product_name: {entry_id: orig_entry}},
            merge=True,
        )
    except Exception:
        pass

    # ── Legacy subcollection STOCK/{groupId}/{productName}/{entryId} ──────────
    try:
        san_name = (product_name or product_id).replace("/", "_").replace("\\", "_").replace(".", "_")
        db.collection(C.STOCK).document(group_id).collection(san_name).document(entry_id).set(orig_entry)
    except Exception:
        pass

    # ── Update product available_stock ────────────────────────────────────────
    stock_updated = False
    closing_stock = quantity

    # Android path first
    bd_prod_ref = _bd_products_ref(db, group_id).document(product_id)
    bd_prod_doc = bd_prod_ref.get()
    if bd_prod_doc.exists:
        current_stock = int(bd_prod_doc.to_dict().get("available_stock", 0) or 0)
        bd_prod_ref.update({
            "available_stock": current_stock + quantity,
            "buying_price": buying_price,
            "unit_price": unit_price,
        })
        closing_stock = current_stock + quantity
        stock_updated = True

    # Flat PRODUCTS collection
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
                    cur_stk = int(prod_data.get("available_stock") or prod_data.get("availableStock") or 0)
                    orig_prod_ref.update({
                        f"{product_id}.available_stock": cur_stk + quantity,
                        f"{product_id}.buying_price": buying_price,
                        f"{product_id}.unit_price": unit_price,
                    })
        except Exception:
            pass

    invalidate_group_payload("stock_in", group_id)
    invalidate_group_payload("stock_in_canonical", group_id)
    invalidate_group_payload("products", group_id)
    invalidate_group_payload("products_canonical", group_id)
    invalidate_report("stock", group_id)
    invalidate_report("stock_canonical", group_id)
    try:
        stock_card = (
            "📦 STOCK ADDITION CARD\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{'Product':<20} | {product_name}\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{'Added Today':<20} | {quantity:.0f} units\n"
            f"{'New Stock Level':<20} | {closing_stock:.0f} units\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "✅ Stock successfully updated"
        )
        post_group_event_message(
            db,
            uid,
            group_id,
            stock_card,
            sender_name_override="Stock Manager",
        )
    except Exception:
        pass
    return jsonify({"stockIn": stock_in_to_dict(entry_id, entry_data)}), 201


@stock_bp.route("/out", methods=["GET"])
@require_auth
def list_stock_out(group_id):
    uid = get_jwt_identity()
    db = get_db()
    canonical_only = _is_true(request.args.get("canonical"))
    is_mem, _ = cached_is_member(
        group_id,
        uid,
        lambda: (_is_member(db, group_id, uid), None),
    )
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    cache_name = "stock_out_canonical" if canonical_only else "stock_out"
    cached_payload = get_cached_group_payload(cache_name, group_id)
    if cached_payload is not None:
        return jsonify(cached_payload)

    entry_map = {}

    # ── Source 1: Android path — BUSINESS_DATA/{groupId}/stock_movements (type=out) ──
    try:
        for d in _bd_movements_ref(db, group_id).where("movementType", "==", "out").get():
            entry_map[d.id] = stock_out_to_dict(d.id, d.to_dict())
    except Exception:
        pass

    # ── Source 2: flat STOCK_OUT collection ───────────────────────────────────
    try:
        docs = db.collection(C.STOCK_OUT).where("group_id", "==", group_id).get()
        for d in docs:
            if d.id not in entry_map:
                entry_map[d.id] = stock_out_to_dict(d.id, d.to_dict())
    except Exception:
        pass

    if not canonical_only:
        # ── Source 3: Windows Flutter format — STOCK_OUT/{productName} ───────
        so2_names = set()
        try:
            for pd in db.collection(C.PRODUCTS).where("group_id", "==", group_id).get():
                name = (pd.to_dict() or {}).get("name", "")
                if name:
                    so2_names.add(name)
        except Exception:
            pass
        try:
            opd = db.collection(C.PRODUCTS).document(group_id).get()
            if opd.exists:
                for opv in (opd.to_dict() or {}).values():
                    if isinstance(opv, dict):
                        name = opv.get("name", "")
                        if name:
                            so2_names.add(name)
        except Exception:
            pass

        for product_name in so2_names:
            try:
                so2 = db.collection(C.STOCK_OUT).document(product_name).get()
                if so2.exists:
                    for stock_id, val in (so2.to_dict() or {}).items():
                        if isinstance(val, dict) and stock_id not in entry_map:
                            entry_map[stock_id] = stock_out_to_dict(stock_id, val)
            except Exception:
                pass

        # ── Source 4: Android legacy format — STOCK_OUT/{groupId} map doc ─────
        try:
            android_so = db.collection(C.STOCK_OUT).document(group_id).get()
            if android_so.exists:
                for out_id, val in (android_so.to_dict() or {}).items():
                    if isinstance(val, dict) and out_id not in entry_map:
                        entry_map[out_id] = stock_out_to_dict(out_id, val)
        except Exception:
            pass

    entries = sorted(entry_map.values(), key=lambda e: e["date"], reverse=True)
    payload = {"stockOut": entries}
    set_cached_group_payload(cache_name, group_id, payload)
    return jsonify(payload)


@stock_bp.route("/out", methods=["POST"])
@require_auth
def record_stock_out(group_id):
    """
    Manually record a stock-out movement (not tied to a sale).

    Body: {productId, productName, quantity, unitPrice, buyingPrice, measuringUnit?, date?}

    Writes to BUSINESS_DATA/{groupId}/stock_movements/{id} with movementType="out"
    and decrements product.available_stock.
    """
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = cached_is_member(
        group_id,
        uid,
        lambda: (_is_member(db, group_id, uid), None),
    )
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    data = request.get_json() or {}
    product_id   = data.get("productId",   "")
    product_name = data.get("productName", "")
    quantity     = int(data.get("quantity", 0))
    if not product_id or quantity <= 0:
        return jsonify({"error": "productId and quantity are required"}), 400

    unit_price   = float(data.get("unitPrice",   0))
    buying_price = float(data.get("buyingPrice", 0))
    meas_unit    = data.get("measuringUnit", "pcs")
    now          = int(data.get("date") or datetime.now(timezone.utc).timestamp() * 1000)

    movement_id = str(uuid.uuid4())
    movement_data = {
        "group_id":       group_id,
        "groupId":        group_id,
        "product_id":     product_id,
        "productId":      product_id,
        "name":           product_name,
        "unit_price":     unit_price,
        "buying_price":   buying_price,
        "measuring_unit": meas_unit,
        "quantity":       quantity,
        "movementType":   "out",
        "sale_id":        "",
        "date":           now,
        "id":             movement_id,
    }

    # ── Primary: Android path BUSINESS_DATA/{groupId}/stock_movements/{id} ───
    _bd_movements_ref(db, group_id).document(movement_id).set(movement_data)

    # ── Legacy: flat STOCK_OUT ────────────────────────────────────────────────
    try:
        db.collection(C.STOCK_OUT).document(movement_id).set({
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
    except Exception:
        pass

    # ── Legacy: STOCK_OUT/{groupId} map doc ───────────────────────────────────
    try:
        db.collection(C.STOCK_OUT).document(group_id).set(
            {movement_id: movement_data}, merge=True
        )
    except Exception:
        pass

    # ── Decrement product stock ───────────────────────────────────────────────
    # Android path
    bd_prod_ref = (
        db.collection(C.BUSINESS_DATA)
        .document(group_id)
        .collection(C.BD_PRODUCTS)
        .document(product_id)
    )
    bd_prod_doc = bd_prod_ref.get()
    if bd_prod_doc.exists:
        current = int(bd_prod_doc.to_dict().get("available_stock", 0) or 0)
        bd_prod_ref.update({"available_stock": max(0, current - quantity)})

    # Flat PRODUCTS collection
    try:
        prod_doc = db.collection(C.PRODUCTS).document(product_id).get()
        if prod_doc.exists and prod_doc.to_dict().get("group_id") == group_id:
            current = int(prod_doc.to_dict().get("available_stock", 0) or 0)
            prod_doc.reference.update({"available_stock": max(0, current - quantity)})
    except Exception:
        pass

    invalidate_group_payload("stock_out", group_id)
    invalidate_group_payload("stock_out_canonical", group_id)
    invalidate_group_payload("products", group_id)
    invalidate_group_payload("products_canonical", group_id)
    invalidate_report("stock", group_id)
    invalidate_report("stock_canonical", group_id)

    return jsonify({"movement": _movement_to_dict(movement_id, movement_data)}), 201


@stock_bp.route("/movements", methods=["GET"])
@require_auth
def list_movements(group_id):
    """
    List ALL stock movements (in + out) from BUSINESS_DATA/{groupId}/stock_movements.

    Query params:
      ?type=in|out        — filter by movementType (default: all)
      ?start=<ms epoch>   — minimum date (inclusive)
      ?end=<ms epoch>     — maximum date (inclusive)
    """
    uid = get_jwt_identity()
    db = get_db()
    is_mem, _ = cached_is_member(
        group_id,
        uid,
        lambda: (_is_member(db, group_id, uid), None),
    )
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    movement_type = request.args.get("type", "").strip().lower()
    start_ms = request.args.get("start", "")
    end_ms   = request.args.get("end",   "")

    try:
        start_ms = int(start_ms) if start_ms else None
    except ValueError:
        start_ms = None
    try:
        end_ms = int(end_ms) if end_ms else None
    except ValueError:
        end_ms = None

    query = _bd_movements_ref(db, group_id)
    if movement_type in {"in", "out"}:
        query = query.where("movementType", "==", movement_type)
    if start_ms is not None:
        query = query.where("date", ">=", start_ms)
    if end_ms is not None:
        query = query.where("date", "<=", end_ms)

    try:
        docs = query.get()
    except Exception as e:
        return jsonify({"error": f"Query failed: {str(e)}"}), 500

    movements = sorted(
        [_movement_to_dict(d.id, d.to_dict()) for d in docs],
        key=lambda m: m["date"],
        reverse=True,
    )
    return jsonify({"movements": movements, "count": len(movements)})
