"""
Dashboard endpoint — aggregated business intelligence for a group.

GET /groups/<group_id>/dashboard

Returns:
  {
    "todaySales":       {"count": int, "total": float},
    "weekSales":        {"count": int, "total": float},
    "monthSales":       {"count": int, "total": float},
    "lowStockProducts": [{id, name, available_stock, reorder_level}, ...],
    "topProducts":      [{id, name, totalSold, revenue}, ...],
    "totalCustomers":   int,
    "outstandingCredit":float,
    "recentSales":      [last 5 sales]
  }

Primary data source: BUSINESS_DATA/{groupId}/* (Android path).
Falls back to flat collections for groups not yet on the Android path.
"""

from flask import Blueprint, jsonify
from firebase_utils import get_db
from auth_utils import require_auth, get_jwt_identity
from models import sale_to_dict, product_to_dict, customer_to_dict
import db_constants as C
import logging
from datetime import datetime, timezone, timedelta
from cache_utils import cached_is_member, get_cached_group_payload, set_cached_group_payload

dashboard_bp = Blueprint("dashboard", __name__, url_prefix="/groups/<group_id>/dashboard")

# Dashboard payload is cached for 60 s (handled by cache_utils TTL if supported;
# otherwise we just set it and let the next request after invalidation refresh it).
_CACHE_KEY = "dashboard"


def _check_member(db, group_id, uid):
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
            return True, None
    except Exception:
        pass
    try:
        chats_doc = db.collection(C.CHATS).document(uid).get()
        if chats_doc.exists:
            cd = chats_doc.to_dict() or {}
            if group_id in cd and isinstance(cd[group_id], dict):
                return True, None
    except Exception:
        pass
    return False, None


def _ms_bounds():
    """Return (today_start_ms, week_start_ms, month_start_ms) in UTC."""
    now = datetime.now(timezone.utc)
    today_start  = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    week_start   = today_start - timedelta(days=now.weekday())
    month_start  = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
    to_ms = lambda dt: int(dt.timestamp() * 1000)
    return to_ms(today_start), to_ms(week_start), to_ms(month_start)


def _fetch_all_sales(db, group_id):
    """
    Return a list of raw sale dicts from all available sources, deduplicated by doc id.
    Each dict carries the document id as '_id'.
    """
    seen = {}

    # Primary: Android BUSINESS_DATA/{groupId}/sales
    try:
        for d in (
            db.collection(C.BUSINESS_DATA)
            .document(group_id)
            .collection(C.BD_SALES)
            .get()
        ):
            dd = d.to_dict() or {}
            dd["_id"] = d.id
            seen[d.id] = dd
    except Exception as e:
        logging.exception("dashboard _fetch_all_sales Android: %s", e)

    # Fallback: flat CASH_SALE / CREDIT_SALE
    for coll_name, is_credit_flag in [(C.CASH_SALE, False), (C.CREDIT_SALE, True)]:
        try:
            for d in db.collection(coll_name).where("group_id", "==", group_id).get():
                if d.id not in seen:
                    dd = d.to_dict() or {}
                    dd.setdefault("is_credit", is_credit_flag)
                    dd["_id"] = d.id
                    seen[d.id] = dd
        except Exception:
            pass

    return list(seen.values())


def _fetch_all_products(db, group_id):
    """Return {product_id: raw_dict} from Android + flat sources."""
    product_map = {}

    # Android path
    try:
        for d in (
            db.collection(C.BUSINESS_DATA)
            .document(group_id)
            .collection(C.BD_PRODUCTS)
            .get()
        ):
            product_map[d.id] = d.to_dict() or {}
    except Exception:
        pass

    # Flat PRODUCTS
    try:
        for d in db.collection(C.PRODUCTS).where("group_id", "==", group_id).get():
            if d.id not in product_map:
                product_map[d.id] = d.to_dict() or {}
    except Exception:
        pass

    return product_map


def _fetch_customer_stats(db, group_id):
    """Return (total_customers, outstanding_credit)."""
    cust_map = {}

    # Android path
    try:
        for d in (
            db.collection(C.BUSINESS_DATA)
            .document(group_id)
            .collection(C.BD_CUSTOMERS)
            .get()
        ):
            cust_map[d.id] = d.to_dict() or {}
    except Exception:
        pass

    # Flat
    try:
        for d in db.collection(C.CUSTOMERS).where("group_id", "==", group_id).get():
            if d.id not in cust_map:
                cust_map[d.id] = d.to_dict() or {}
    except Exception:
        pass

    outstanding = sum(
        float(c.get("balance") or c.get("totalDebt") or 0.0)
        for c in cust_map.values()
        if float(c.get("balance") or c.get("totalDebt") or 0.0) > 0
    )
    return len(cust_map), outstanding


@dashboard_bp.route("", methods=["GET"])
@require_auth
def get_dashboard(group_id):
    uid = get_jwt_identity()
    db  = get_db()

    is_mem, _ = cached_is_member(group_id, uid, lambda: _check_member(db, group_id, uid))
    if not is_mem:
        return jsonify({"error": "Access denied"}), 403

    # Short-circuit with cached payload if available
    cached = get_cached_group_payload(_CACHE_KEY, group_id)
    if cached is not None:
        return jsonify(cached)

    today_ms, week_ms, month_ms = _ms_bounds()

    # ── 1. Collect all sales ──────────────────────────────────────────────────
    all_sales = _fetch_all_sales(db, group_id)

    # Aggregate time-windowed totals
    today_count  = today_total  = 0
    week_count   = week_total   = 0
    month_count  = month_total  = 0

    # Track product-level sales for topProducts
    product_stats = {}  # {product_id: {name, totalSold, revenue}}

    for s in all_sales:
        date       = int(s.get("date", 0) or 0)
        unit_price = float(s.get("unit_price") or s.get("unitPrice") or 0)
        quantity   = int(s.get("quantity", 1) or 1)
        line_total = unit_price * quantity
        prod_id    = s.get("product_id") or s.get("productId") or ""
        prod_name  = s.get("product_name") or s.get("name") or ""

        if date >= month_ms:
            month_count += 1
            month_total += line_total
        if date >= week_ms:
            week_count += 1
            week_total += line_total
        if date >= today_ms:
            today_count += 1
            today_total += line_total

        if prod_id:
            if prod_id not in product_stats:
                product_stats[prod_id] = {"id": prod_id, "name": prod_name, "totalSold": 0, "revenue": 0.0}
            product_stats[prod_id]["totalSold"] += quantity
            product_stats[prod_id]["revenue"]   += line_total

    top_products = sorted(
        product_stats.values(),
        key=lambda p: p["revenue"],
        reverse=True,
    )[:10]

    # ── 2. Recent sales (last 5 by date) ─────────────────────────────────────
    recent_sales = sorted(all_sales, key=lambda s: int(s.get("date", 0) or 0), reverse=True)[:5]
    recent_sales_out = [
        sale_to_dict(s["_id"], s)
        for s in recent_sales
    ]

    # ── 3. Products — low stock ───────────────────────────────────────────────
    product_map  = _fetch_all_products(db, group_id)
    low_stock = []
    for pid, pd in product_map.items():
        avail   = int(pd.get("available_stock") or pd.get("availableStock") or 0)
        reorder = int(pd.get("reorder_level")   or pd.get("reorderLevel")   or 10)
        if avail <= reorder:
            low_stock.append({
                "id":              pid,
                "name":            pd.get("name", ""),
                "available_stock": avail,
                "reorder_level":   reorder,
                # Flutter LowStockAlert.fromJson reads 'measuring_unit'
                "measuring_unit":  pd.get("measuring_unit") or pd.get("measuringUnit") or "pcs",
            })
    low_stock.sort(key=lambda p: p["available_stock"])

    # Rename totalSold → quantity so Flutter TopProduct.fromJson can read it
    top_products_out = [
        {"id": p["id"], "name": p["name"], "quantity": p["totalSold"], "revenue": p["revenue"]}
        for p in top_products
    ]

    # ── 4. Customer stats ─────────────────────────────────────────────────────
    total_customers, outstanding_credit = _fetch_customer_stats(db, group_id)

    # ── 5. Build payload ──────────────────────────────────────────────────────
    # Key names match Flutter DashboardData.fromJson:
    #   json['today'], json['week'], json['month'], json['lowStockAlerts'], json['outstandingCredit']
    payload = {
        "today":             {"count": today_count,  "total": round(today_total,  2)},
        "week":              {"count": week_count,   "total": round(week_total,   2)},
        "month":             {"count": month_count,  "total": round(month_total,  2)},
        "lowStockAlerts":    low_stock,
        "topProducts":       top_products_out,
        "totalCustomers":    total_customers,
        "outstandingCredit": round(outstanding_credit, 2),
        "recentSales":       recent_sales_out,
    }

    set_cached_group_payload(_CACHE_KEY, group_id, payload)
    return jsonify(payload)
