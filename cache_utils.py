"""
Simple in-memory TTL caches to reduce repeated Firestore reads.

These caches live in the worker process memory and are intentionally
lightweight — no external dependencies, no persistence.
"""
import time
import threading

_lock = threading.Lock()

# ── Member-auth cache ──────────────────────────────────────────────────────────
# Key: (group_id, uid)  →  (is_member: bool, admin_id: str|None, expiry: float)
_MEMBER_TTL = 300  # 5 minutes
_member_cache: dict = {}

def cached_is_member(group_id: str, uid: str, check_fn):
    """Return (is_member, admin_id), using a 5-min in-process cache."""
    key = (group_id, uid)
    now = time.monotonic()
    with _lock:
        entry = _member_cache.get(key)
        if entry and now < entry[2]:
            return entry[0], entry[1]
    result, admin_id = check_fn()
    with _lock:
        _member_cache[key] = (result, admin_id, now + _MEMBER_TTL)
    return result, admin_id

def invalidate_member(group_id: str, uid: str):
    """Call after group membership changes to force a re-check."""
    with _lock:
        _member_cache.pop((group_id, uid), None)

# ── Product-list response cache ────────────────────────────────────────────────
# Key: group_id  →  (products_list, expiry: float)
_PRODUCT_TTL = 25  # seconds — slightly less than Flutter's 30 s poll interval
_product_cache: dict = {}

def get_cached_products(group_id: str):
    """Return cached product list or None if stale/missing."""
    now = time.monotonic()
    with _lock:
        entry = _product_cache.get(group_id)
        if entry and now < entry[1]:
            return entry[0]
    return None

def set_cached_products(group_id: str, products: list):
    with _lock:
        _product_cache[group_id] = (products, time.monotonic() + _PRODUCT_TTL)

def invalidate_products(group_id: str):
    """Call after any product mutation so next poll gets fresh data."""
    with _lock:
        _product_cache.pop(group_id, None)

# -- Report response cache -----------------------------------------------------
# Key: (report_type, group_id) -> (payload, expiry: float)
_REPORT_TTL = 20
_report_cache: dict = {}

def get_cached_report(report_type: str, group_id: str):
    now = time.monotonic()
    with _lock:
        entry = _report_cache.get((report_type, group_id))
        if entry and now < entry[1]:
            return entry[0]
    return None

def set_cached_report(report_type: str, group_id: str, payload):
    with _lock:
        _report_cache[(report_type, group_id)] = (payload, time.monotonic() + _REPORT_TTL)

def invalidate_report(report_type: str, group_id: str):
    with _lock:
        _report_cache.pop((report_type, group_id), None)
