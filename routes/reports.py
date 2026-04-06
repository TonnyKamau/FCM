"""
Report endpoints for the kit-ifms Flutter app.

  GET /groups/<group_id>/reports/sales
  GET /groups/<group_id>/reports/stock
  GET /groups/<group_id>/reports/expenses
  GET /groups/<group_id>/reports/income
"""

from flask import Blueprint, jsonify
from firebase_utils import get_db
from models import sale_to_dict, stock_in_to_dict, stock_out_to_dict
from auth_utils import require_auth, get_jwt_identity
from cache_utils import get_cached_report, set_cached_report
from routes.expenses import _list as _expenses_list
import db_constants as C

reports_bp = Blueprint("reports", __name__, url_prefix="/groups/<group_id>/reports")


def _is_member(db, group_id, uid):
    # â”€â”€ New backend: GroupAccounts + GroupMembers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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
    # â”€â”€ Original project: CHATS/{uid} map contains the group_id key â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    try:
        chats_doc = db.collection(C.CHATS).document(uid).get()
        if chats_doc.exists:
            chat_data = chats_doc.to_dict() or {}
            if group_id in chat_data and isinstance(chat_data[group_id], dict):
                return True
    except Exception:
        pass
    return False


@reports_bp.route("/sales", methods=["GET"])
@require_auth
def sales_report(group_id):
    uid = get_jwt_identity()
    db = get_db()
    if not _is_member(db, group_id, uid):
        return jsonify({"error": "Access denied"}), 403

    # â”€â”€ Source 1: new backend â€” flat CASH_SALE / CREDIT_SALE collections â”€â”€â”€â”€â”€â”€
    cached_payload = get_cached_report('sales', group_id)
    if cached_payload is not None:
        return jsonify(cached_payload)

    cash_docs   = db.collection(C.CASH_SALE  ).where("group_id", "==", group_id).get()
    credit_docs = db.collection(C.CREDIT_SALE).where("group_id", "==", group_id).get()
    cash_map   = {d.id: sale_to_dict(d.id, d.to_dict()) for d in cash_docs}
    credit_map = {d.id: sale_to_dict(d.id, d.to_dict()) for d in credit_docs}

    # â”€â”€ Source 2: original project â€” nested subcollection structure â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # IMPORTANT: use list_documents() NOT stream() for the product-name level.
    for coll_name, sale_map, is_credit_flag in [
        (C.CASH_SALE, cash_map, False), (C.CREDIT_SALE, credit_map, True)
    ]:
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
            import logging
            logging.exception("sales_report Source 2 error (%s %s): %s", coll_name, group_id, e)

    cash_sales   = sorted(cash_map.values(),   key=lambda s: s["date"], reverse=True)
    credit_sales = sorted(credit_map.values(), key=lambda s: s["date"], reverse=True)
    all_sales = sorted(
        list(cash_map.values()) + list(credit_map.values()),
        key=lambda s: s["date"], reverse=True
    )
    payload = {
        "sales":       all_sales,
        "cashSales":   cash_sales,
        "creditSales": credit_sales,
    }
    set_cached_report('sales', group_id, payload)
    return jsonify(payload)


@reports_bp.route("/stock", methods=["GET"])
@require_auth
def stock_report(group_id):
    uid = get_jwt_identity()
    db = get_db()
    if not _is_member(db, group_id, uid):
        return jsonify({"error": "Access denied"}), 403

    # â”€â”€ Source 1: new backend â€” flat STOCK / STOCK_OUT collections â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    cached_payload = get_cached_report('stock', group_id)
    if cached_payload is not None:
        return jsonify(cached_payload)

    stock_in_docs  = db.collection(C.STOCK    ).where("group_id", "==", group_id).get()
    stock_out_docs = db.collection(C.STOCK_OUT).where("group_id", "==", group_id).get()
    in_map  = {d.id: stock_in_to_dict (d.id, d.to_dict()) for d in stock_in_docs}
    out_map = {d.id: stock_out_to_dict(d.id, d.to_dict()) for d in stock_out_docs}

    # â”€â”€ Source 2a: Windows Flutter â€” STOCK/{groupId} map document â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    try:
        stock_doc = db.collection(C.STOCK).document(group_id).get()
        if stock_doc.exists:
            for key, val in (stock_doc.to_dict() or {}).items():
                if not isinstance(val, dict):
                    continue
                if "id" in val and "name" in val:
                    if key not in in_map:
                        in_map[key] = stock_in_to_dict(key, val)
                else:
                    for stock_id, stock_entry in val.items():
                        if isinstance(stock_entry, dict) and stock_id not in in_map:
                            in_map[stock_id] = stock_in_to_dict(stock_id, stock_entry)
    except Exception as e:
        import logging
        logging.exception("stock_report Source 2a (STOCK map) error (%s): %s", group_id, e)

    # â”€â”€ Source 2a-extra: Android app â€” STOCK/{groupId}/{productName}/{stockId} â”€â”€
    try:
        grp_stock_ref = db.collection(C.STOCK).document(group_id)
        for sub_coll in grp_stock_ref.collections():
            for d in sub_coll.stream():
                if d.id not in in_map:
                    in_map[d.id] = stock_in_to_dict(d.id, d.to_dict() or {})
    except Exception as e:
        import logging
        logging.exception("stock_report Source 2a-extra (Android STOCK) error (%s): %s", group_id, e)

    # â”€â”€ Source 2b: original project â€” STOCK_OUT/{productName} map documents â”€â”€â”€â”€â”€
    so2b_names: set = set()
    try:
        for pd in db.collection(C.PRODUCTS).where("group_id", "==", group_id).get():
            n = (pd.to_dict() or {}).get("name", "")
            if n:
                so2b_names.add(n)
    except Exception:
        pass
    try:
        opd = db.collection(C.PRODUCTS).document(group_id).get()
        if opd.exists:
            for opv in (opd.to_dict() or {}).values():
                if isinstance(opv, dict):
                    n = opv.get("name", "")
                    if n:
                        so2b_names.add(n)
    except Exception:
        pass
    for pn in so2b_names:
        try:
            so2b = db.collection(C.STOCK_OUT).document(pn).get()
            if so2b.exists:
                for stock_id, val in (so2b.to_dict() or {}).items():
                    if isinstance(val, dict) and stock_id not in out_map:
                        out_map[stock_id] = stock_out_to_dict(stock_id, val)
        except Exception as e:
            import logging
            logging.exception(
                "stock_report Source 2b (STOCK_OUT) error (%s, %s): %s", group_id, pn, e
            )

    # â”€â”€ Source 2b-extra: Android app â€” STOCK_OUT/{groupId} map document â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    try:
        android_so_doc = db.collection(C.STOCK_OUT).document(group_id).get()
        if android_so_doc.exists:
            for out_id, val in (android_so_doc.to_dict() or {}).items():
                if isinstance(val, dict) and out_id not in out_map:
                    out_map[out_id] = stock_out_to_dict(out_id, val)
    except Exception as e:
        import logging
        logging.exception("stock_report Source 2b-extra (Android STOCK_OUT) error (%s): %s", group_id, e)

    stock_in  = sorted(in_map.values(),  key=lambda e: e["date"], reverse=True)
    stock_out = sorted(out_map.values(), key=lambda e: e["date"], reverse=True)
    payload = {"stockIn": stock_in, "stockOut": stock_out}
    set_cached_report('stock', group_id, payload)
    return jsonify(payload)


@reports_bp.route("/expenses", methods=["GET"])
@require_auth
def expenses_report(group_id):
    """Returns all expense entries for the group.  JSON: {"expenses": [...]}"""
    return _expenses_list(group_id, True)


@reports_bp.route("/income", methods=["GET"])
@require_auth
def income_report(group_id):
    """Returns all income entries for the group.   JSON: {"incomes": [...]}"""
    return _expenses_list(group_id, False)


