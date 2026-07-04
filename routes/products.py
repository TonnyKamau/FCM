from flask import Blueprint, request, jsonify
from firebase_utils import get_db
from models import product_to_dict
from auth_utils import require_auth, get_jwt_identity
import db_constants as C
import uuid
from datetime import datetime, timezone
from cache_utils import (
    cached_is_member, invalidate_products,
    get_cached_products, set_cached_products,
    get_cached_group_payload, set_cached_group_payload, invalidate_group_payload,
)

products_bp = Blueprint("products", __name__, url_prefix="/groups/<group_id>/products")


def _valid_image_value(value):
    """Only http(s) URLs (or empty to clear) — Android's Glide loaders reject
    base64/local paths, so persisting them would break cross-platform display."""
    return value == "" or str(value).startswith(("http://", "https://"))




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


def _bd_products_ref(db, group_id):
    """Shorthand for BUSINESS_DATA/{groupId}/products subcollection."""
    return (
        db.collection(C.BUSINESS_DATA)
        .document(group_id)
        .collection(C.BD_PRODUCTS)
    )


@products_bp.route("", methods=["GET"])
@require_auth
def list_products(group_id):
    uid = get_jwt_identity()
    db = get_db()
    canonical_only = _is_true(request.args.get("canonical"))
    is_mem, _ = cached_is_member(group_id, uid, lambda: (_is_member(db, group_id, uid), None))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    # Return cached response if fresh (avoids repeated dual-source reads)
    if canonical_only:
        cached_payload = get_cached_group_payload("products_canonical", group_id)
        if cached_payload is not None:
            return jsonify(cached_payload)
    else:
        cached = get_cached_products(group_id)
        if cached is not None:
            return jsonify({"products": cached})

    product_map = {}

    # ── Source 1: Android path — BUSINESS_DATA/{groupId}/products ────────────
    try:
        bd_docs = _bd_products_ref(db, group_id).get()
        for d in bd_docs:
            product_map[d.id] = product_to_dict(d.id, d.to_dict())
    except Exception:
        pass

    # ── Source 2: new backend — flat PRODUCTS collection with group_id field ──
    try:
        docs = db.collection(C.PRODUCTS).where("group_id", "==", group_id).get()
        for d in docs:
            if d.id not in product_map:
                product_map[d.id] = product_to_dict(d.id, d.to_dict())
    except Exception:
        pass

    # ── Source 3: original project — PRODUCTS/{groupId} single map document ──
    if not canonical_only:
        try:
            orig_doc = db.collection(C.PRODUCTS).document(group_id).get()
            if orig_doc.exists:
                for prod_id, prod_data in (orig_doc.to_dict() or {}).items():
                    if not isinstance(prod_data, dict):
                        continue
                    if prod_id not in product_map:
                        product_map[prod_id] = product_to_dict(prod_id, prod_data)
                    else:
                        android_parsed = product_to_dict(prod_id, prod_data)
                        flat = product_map[prod_id]

                        merge_fields = [
                            ("available_stock", "available_stock"),
                            ("image",           "image"),
                            ("unit_price",      "unit_price"),
                            ("buying_price",    "buying_price"),
                            ("wholesale_price", "wholesale_price"),
                            ("special_price",   "special_price"),
                            ("reorder_level",   "reorder_level"),
                            ("name",            "name"),
                            ("desc",            "desc"),
                        ]
                        backfill = {}
                        for resp_key, _ in merge_fields:
                            android_val = android_parsed.get(resp_key)
                            flat_val    = flat.get(resp_key)
                            if android_val is not None and android_val != flat_val:
                                flat[resp_key] = android_val
                                fs_key = {
                                    "available_stock": "available_stock",
                                    "image":           "image",
                                    "unit_price":      "unit_price",
                                    "buying_price":    "buying_price",
                                    "wholesale_price": "wholesale_price",
                                    "special_price":   "special_price",
                                    "reorder_level":   "reorder_level",
                                    "name":            "name",
                                    "desc":            "description",
                                }.get(resp_key, resp_key)
                                backfill[fs_key] = android_val

                        if backfill:
                            try:
                                db.collection(C.PRODUCTS).document(prod_id).update(backfill)
                            except Exception:
                                pass
        except Exception:
            pass

    products = sorted(product_map.values(), key=lambda p: p["name"])
    payload = {"products": products}
    if canonical_only:
        set_cached_group_payload("products_canonical", group_id, payload)
    else:
        set_cached_products(group_id, products)
    return jsonify(payload)


@products_bp.route("", methods=["POST"])
@require_auth
def create_product(group_id):
    uid = get_jwt_identity()
    db = get_db()
    if not _is_member(db, group_id, uid):
        return jsonify({"error": "Access denied"}), 403

    data = request.get_json() or {}
    name = data.get("name", "").strip()
    if not name:
        return jsonify({"error": "name is required"}), 400

    if not _valid_image_value(data.get("image", "")):
        return jsonify({"error": "image must be an uploaded URL; use POST /photos/upload first"}), 400

    product_id = data.get("id") or str(uuid.uuid4())
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    product_data = {
        "group_id":        group_id,
        "name":            name,
        "description":     data.get("desc", ""),
        "image":           data.get("image", ""),
        "buying_price":    float(data.get("buying_price",    0)),
        "unit_price":      float(data.get("unit_price",      0)),
        "available_stock": int  (data.get("available_stock", 0)),
        "reorder_level":   int  (data.get("reorder_level",  10)),
        "measuring_unit":  data.get("measuring_unit", "pcs"),
        "category":        data.get("category", ""),
        "created_at":      int(data.get("date", now)),
        "barcode":         data.get("barcode", ""),
        "code":            data.get("code", ""),
        "wholesale_price": float(data.get("wholesale_price", 0) or data.get("wholesalePrice", 0) or 0),
        "special_price":   float(data.get("special_price",   0) or data.get("specialPrice",   0) or 0),
        "tax_rate":        float(data.get("tax_rate",     16.0) or data.get("taxRate",     16.0) or 16.0),
        "is_active":       bool (data.get("is_active",    True) if "is_active" in data else data.get("isActive", True)),
    }

    # ── Primary write: Android path BUSINESS_DATA/{groupId}/products/{productId} ──
    _bd_products_ref(db, group_id).document(product_id).set(product_data)

    # ── Legacy write: flat PRODUCTS collection (new backend) ─────────────────
    try:
        db.collection(C.PRODUCTS).document(product_id).set(product_data)
    except Exception:
        pass

    # Also write into the Android map-document format so the Android app sees it
    # PRODUCTS/{groupId} is a single document whose keys are product IDs
    android_data = {
        "name":            product_data["name"],
        "description":     product_data["description"],
        "image":           product_data["image"],
        "buying_price":    product_data["buying_price"],
        "unit_price":      product_data["unit_price"],
        "available_stock": product_data["available_stock"],
        "reorder_level":   product_data["reorder_level"],
        "measuring_unit":  product_data["measuring_unit"],
        "category":        product_data["category"],
        "date":            product_data["created_at"],
        "barcode":         product_data["barcode"],
        "code":            product_data["code"],
        "wholesale_price": product_data["wholesale_price"],
        "special_price":   product_data["special_price"],
        "tax_rate":        product_data["tax_rate"],
        "is_active":       product_data["is_active"],
        "id":              product_id,
    }
    try:
        db.collection(C.PRODUCTS).document(group_id).set(
            {product_id: android_data}, merge=True
        )
    except Exception:
        pass  # non-fatal: Flutter app still works via flat docs

    invalidate_products(group_id)
    invalidate_group_payload("products_canonical", group_id)
    return jsonify({"product": product_to_dict(product_id, product_data)}), 201


@products_bp.route("/<product_id>", methods=["PUT"])
@require_auth
def update_product(group_id, product_id):
    uid = get_jwt_identity()
    db = get_db()
    if not _is_member(db, group_id, uid):
        return jsonify({"error": "Access denied"}), 403

    # Try Android path first, then flat collection
    bd_ref = _bd_products_ref(db, group_id).document(product_id)
    bd_doc = bd_ref.get()
    if bd_doc.exists:
        doc = bd_doc
    else:
        doc = db.collection(C.PRODUCTS).document(product_id).get()
        if not doc.exists or doc.to_dict().get("group_id") != group_id:
            return jsonify({"error": "Product not found"}), 404

    data = request.get_json() or {}
    updates = {}
    for req_key, db_key in [
        ("name", "name"), ("desc", "description"), ("image", "image"),
        ("category", "category"), ("measuring_unit", "measuring_unit"),
        ("barcode", "barcode"), ("code", "code"),
    ]:
        if req_key in data:
            updates[db_key] = data[req_key]

    image_value = updates.get("image")
    if image_value not in (None, "") and not str(image_value).startswith(("http://", "https://")):
        return jsonify({"error": "image must be an uploaded URL; use POST /photos/upload first"}), 400
    for req_key, db_key in [
        ("buying_price", "buying_price"), ("unit_price", "unit_price"),
        ("wholesale_price", "wholesale_price"), ("wholesalePrice", "wholesale_price"),
        ("special_price", "special_price"),   ("specialPrice",   "special_price"),
        ("tax_rate", "tax_rate"),             ("taxRate",        "tax_rate"),
    ]:
        if req_key in data:
            updates[db_key] = float(data[req_key])
    for req_key, db_key in [
        ("available_stock", "available_stock"), ("reorder_level", "reorder_level"),
    ]:
        if req_key in data:
            updates[db_key] = int(data[req_key])
    for req_key, db_key in [("is_active", "is_active"), ("isActive", "is_active")]:
        if req_key in data:
            updates[db_key] = bool(data[req_key])

    doc.reference.update(updates)

    # Mirror updates to Android path
    try:
        if not bd_doc.exists:
            bd_ref.set({**doc.to_dict(), **updates}, merge=True)
        else:
            bd_ref.update(updates)
    except Exception:
        pass

    # Mirror update into flat PRODUCTS collection
    try:
        flat_doc = db.collection(C.PRODUCTS).document(product_id).get()
        if flat_doc.exists:
            flat_doc.reference.update(updates)
    except Exception:
        pass

    # Mirror update into Android map-document format
    try:
        android_updates = {f"{product_id}.{k}": v for k, v in updates.items()}
        db.collection(C.PRODUCTS).document(group_id).update(android_updates)
    except Exception:
        pass

    invalidate_products(group_id)
    invalidate_group_payload("products_canonical", group_id)
    updated = doc.reference.get()
    return jsonify({"product": product_to_dict(updated.id, updated.to_dict())})


@products_bp.route("/<product_id>", methods=["DELETE"])
@require_auth
def delete_product(group_id, product_id):
    uid = get_jwt_identity()
    db = get_db()
    if not _is_member(db, group_id, uid):
        return jsonify({"error": "Access denied"}), 403

    # Delete from Android path
    bd_ref = _bd_products_ref(db, group_id).document(product_id)
    bd_doc = bd_ref.get()
    found = False
    if bd_doc.exists:
        bd_ref.delete()
        found = True

    # Delete from flat PRODUCTS collection
    flat_doc = db.collection(C.PRODUCTS).document(product_id).get()
    if flat_doc.exists and flat_doc.to_dict().get("group_id") == group_id:
        flat_doc.reference.delete()
        found = True

    if not found:
        return jsonify({"error": "Product not found"}), 404

    # Remove from Android map-document format too
    try:
        from google.cloud.firestore import DELETE_FIELD
        db.collection(C.PRODUCTS).document(group_id).update({product_id: DELETE_FIELD})
    except Exception:
        pass

    invalidate_products(group_id)
    invalidate_group_payload("products_canonical", group_id)
    return jsonify({"message": "Product deleted"})


@products_bp.route("/<product_id>/adjust-stock", methods=["PUT"])
@require_auth
def adjust_stock(group_id, product_id):
    uid = get_jwt_identity()
    db = get_db()
    if not _is_member(db, group_id, uid):
        return jsonify({"error": "Access denied"}), 403

    # Try Android path first
    bd_ref = _bd_products_ref(db, group_id).document(product_id)
    bd_doc = bd_ref.get()
    if bd_doc.exists:
        doc = bd_doc
    else:
        doc = db.collection(C.PRODUCTS).document(product_id).get()
        if not doc.exists or doc.to_dict().get("group_id") != group_id:
            return jsonify({"error": "Product not found"}), 404

    data = request.get_json() or {}
    delta = int(data.get("delta", 0))
    current_stock = doc.to_dict().get("available_stock", 0)
    new_stock = max(0, current_stock + delta)
    doc.reference.update({"available_stock": new_stock})

    # Mirror to Android path if not already there
    try:
        if not bd_doc.exists:
            bd_ref.set({"available_stock": new_stock}, merge=True)
        else:
            bd_ref.update({"available_stock": new_stock})
    except Exception:
        pass

    # Mirror to flat PRODUCTS doc and legacy map doc
    try:
        flat_doc = db.collection(C.PRODUCTS).document(product_id).get()
        if flat_doc.exists:
            flat_doc.reference.update({"available_stock": new_stock})
    except Exception:
        pass
    try:
        db.collection(C.PRODUCTS).document(group_id).set(
            {product_id: {"available_stock": new_stock}}, merge=True
        )
    except Exception:
        pass

    invalidate_products(group_id)
    invalidate_group_payload("products_canonical", group_id)
    updated = doc.reference.get()
    return jsonify({"product": product_to_dict(updated.id, updated.to_dict())})
