from flask import Blueprint, request, jsonify
from firebase_utils import get_db
from models import product_to_dict
from auth_utils import require_auth, get_jwt_identity
import db_constants as C
import uuid
from datetime import datetime, timezone

products_bp = Blueprint("products", __name__, url_prefix="/groups/<group_id>/products")


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


@products_bp.route("", methods=["GET"])
@require_auth
def list_products(group_id):
    uid = get_jwt_identity()
    db = get_db()
    if not _is_member(db, group_id, uid):
        return jsonify({"error": "Access denied"}), 403

    # ── Source 1: new backend — flat PRODUCTS collection with group_id field ──
    docs = db.collection(C.PRODUCTS).where("group_id", "==", group_id).get()
    product_map = {d.id: product_to_dict(d.id, d.to_dict()) for d in docs}

    # ── Source 2: original project — PRODUCTS/{groupId} single map document ──
    try:
        orig_doc = db.collection(C.PRODUCTS).document(group_id).get()
        if orig_doc.exists:
            for prod_id, prod_data in (orig_doc.to_dict() or {}).items():
                if isinstance(prod_data, dict) and prod_id not in product_map:
                    product_map[prod_id] = product_to_dict(prod_id, prod_data)
    except Exception:
        pass

    products = sorted(product_map.values(), key=lambda p: p["name"])
    return jsonify({"products": products})


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
    db.collection(C.PRODUCTS).document(product_id).set(product_data)
    return jsonify({"product": product_to_dict(product_id, product_data)}), 201


@products_bp.route("/<product_id>", methods=["PUT"])
@require_auth
def update_product(group_id, product_id):
    uid = get_jwt_identity()
    db = get_db()
    if not _is_member(db, group_id, uid):
        return jsonify({"error": "Access denied"}), 403

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
    updated = db.collection(C.PRODUCTS).document(product_id).get()
    return jsonify({"product": product_to_dict(updated.id, updated.to_dict())})


@products_bp.route("/<product_id>", methods=["DELETE"])
@require_auth
def delete_product(group_id, product_id):
    uid = get_jwt_identity()
    db = get_db()
    if not _is_member(db, group_id, uid):
        return jsonify({"error": "Access denied"}), 403
    doc = db.collection(C.PRODUCTS).document(product_id).get()
    if not doc.exists or doc.to_dict().get("group_id") != group_id:
        return jsonify({"error": "Product not found"}), 404
    doc.reference.delete()
    return jsonify({"message": "Product deleted"})


@products_bp.route("/<product_id>/adjust-stock", methods=["PUT"])
@require_auth
def adjust_stock(group_id, product_id):
    uid = get_jwt_identity()
    db = get_db()
    if not _is_member(db, group_id, uid):
        return jsonify({"error": "Access denied"}), 403
    doc = db.collection(C.PRODUCTS).document(product_id).get()
    if not doc.exists or doc.to_dict().get("group_id") != group_id:
        return jsonify({"error": "Product not found"}), 404

    data = request.get_json() or {}
    delta = int(data.get("delta", 0))
    current_stock = doc.to_dict().get("available_stock", 0)
    doc.reference.update({"available_stock": max(0, current_stock + delta)})
    updated = db.collection(C.PRODUCTS).document(product_id).get()
    return jsonify({"product": product_to_dict(updated.id, updated.to_dict())})
