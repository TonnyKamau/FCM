"""
Plain dict helper functions for converting Firestore document data
into the JSON shapes expected by the Flutter app.
No ORM — Firestore documents are just Python dicts.
"""
from datetime import datetime, timezone


def _now_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)


# ── Users ─────────────────────────────────────────────────────────────────────

def user_to_dict(doc_id, d):
    now_iso = datetime.now(timezone.utc).isoformat()
    image_url = d.get("image_url", "") or d.get("image", "")
    return {
        "id": doc_id,
        "name": d.get("name", ""),
        "email": d.get("email", ""),
        "phoneNum": d.get("phone", "") or d.get("phoneNum", ""),
        "countryCode": d.get("country_code", "") or d.get("countryCode", ""),
        "gender": d.get("gender", ""),
        "image": image_url,
        "photoUrl": image_url,
        "role": d.get("role", "user"),
        "isAdmin": d.get("is_admin", False),
        "accountNumber": d.get("account_number", "") or d.get("accountNumber", ""),
        "isActive": d.get("is_active", True),
        "isSelected": d.get("is_selected", False),
        "currentFCMToken": d.get("currentFCMToken", "") or "",
        "reviews": d.get("reviews", None),
        "averageRating": float(d.get("average_rating", 0.0) or 0.0),
        "totalReviews": int(d.get("total_reviews", 0) or 0),
        "isKYCVerified": d.get("is_kyc_verified", False),
        "kycStatus": d.get("kyc_status", "none") or "none",
        "termsAccepted": d.get("terms_accepted", False),
        "termsAcceptedTimestamp": int(d.get("terms_accepted_timestamp", 0) or 0),
        "termsVersion": d.get("terms_version", "") or "",
        "timestamp": int(d.get("timestamp", 0) or 0),
        "borrowerRating": float(d.get("borrower_rating", 0.0) or 0.0),
        "borrowerRatingLevel": d.get("borrower_rating_level", "") or "",
        "totalLoans": int(d.get("total_loans", 0) or 0),
        "completedLoans": int(d.get("completed_loans", 0) or 0),
        "defaultedLoans": int(d.get("defaulted_loans", 0) or 0),
        "onTimeRepaymentRate": float(d.get("on_time_repayment_rate", 0.0) or 0.0),
        "isGuarantor": d.get("is_guarantor", False),
        "guarantorRating": float(d.get("guarantor_rating", 0.0) or 0.0),
        "createdAt": d.get("created_at", now_iso) or now_iso,
        "updatedAt": d.get("updated_at", now_iso) or now_iso,
    }


# ── Groups ────────────────────────────────────────────────────────────────────

def group_member_from_chats(m):
    image = m.get("image", "") or m.get("photoUrl", "") or m.get("imageUrl", "")
    return {
        "id":       m.get("id", "") or m.get("uid", ""),
        "name":     m.get("name", ""),
        "email":    m.get("email", ""),
        "phoneNum": m.get("phoneNum", "") or m.get("phone", ""),
        "image":    image,
        "photoUrl": m.get("photoUrl", "") or image,
        "role":     m.get("role", "UNKNOWN_ROLE"),
    }


def group_to_dict(doc_id, d, members=None):
    if members is None:
        raw = d.get("groupMembers", [])
        if isinstance(raw, list):
            members = [group_member_from_chats(m) for m in raw if isinstance(m, dict)]
        else:
            members = []
    return {
        "id":   doc_id,
        "name": d.get("name", ""),
        "image": d.get("image", ""),
        "adminID":               d.get("admin_id", "")                    or d.get("adminID", ""),
        "isBusinessGroup":       d.get("is_business_group",   True)       if "is_business_group"   in d else d.get("isBusinessGroup",   True),
        "isGroup":               d.get("is_group",            True)       if "is_group"            in d else d.get("isGroup",           True),
        "isMoneyShared":         d.get("is_money_shared",     False)      if "is_money_shared"     in d else d.get("isMoneyShared",     False),
        "restrictMoneyAfterLoanRequest":  d.get("restrict_money_after_loan",    False) if "restrict_money_after_loan"    in d else d.get("restrictMoneyAfterLoanRequest",  False),
        "requireAdminApprovalForLoans":   d.get("require_admin_approval_loans", False) if "require_admin_approval_loans" in d else d.get("requireAdminApprovalForLoans",   False),
        "lastMessage":           d.get("last_message", "")                or d.get("lastMessage", ""),
        "timestamp":             d.get("timestamp", _now_ms()),
        "groupMembers":          members,
        "senderName":            "",
    }


def group_member_to_dict(d, user_doc=None):
    u = user_doc or {}
    image = (
        d.get("member_image", "")
        or d.get("image", "")
        or d.get("member_photo_url", "")
        or d.get("photoUrl", "")
        or u.get("image_url", "")
        or u.get("image", "")
    )
    photo_url = d.get("member_photo_url", "") or d.get("photoUrl", "") or image
    return {
        "id":       d.get("user_id", "") or d.get("id", "") or d.get("uid", ""),
        "name":     d.get("member_name", "") or d.get("name", "") or u.get("name", ""),
        "email":    d.get("member_email", "") or d.get("email", "") or u.get("email", ""),
        "phoneNum": (
            d.get("member_phone", "")
            or d.get("phoneNum", "")
            or d.get("phone", "")
            or u.get("phone", "")
            or u.get("phoneNum", "")
        ),
        "image":    image,
        "photoUrl": photo_url,
        "role":     d.get("role", "UNKNOWN_ROLE"),
    }


# ── Messages ──────────────────────────────────────────────────────────────────

def message_to_dict(doc_id, d):
    try:
        timestamp = int(d.get("timestamp", _now_ms()) or _now_ms())
    except (TypeError, ValueError):
        timestamp = _now_ms()

    return {
        "id":           doc_id,
        "chatID":        d.get("group_id", "")         or d.get("chatID",        ""),
        "senderID":      d.get("sender_id", "")         or d.get("senderID",      ""),
        "senderName":    d.get("sender_name", "")       or d.get("senderName",    ""),
        "receiverID":    d.get("receiver_id", "")       or d.get("receiverID",    ""),
        "receiverName":  d.get("receiver_name", "")     or d.get("receiverName",  ""),
        "message":       d.get("message", ""),
        "isGroup":       d.get("is_group")       if "is_group"       in d else d.get("isGroup",       True),
        "isMoneyShared": d.get("is_money_shared") if "is_money_shared" in d else d.get("isMoneyShared", False),
        "isImageShared": d.get("is_image_shared") if "is_image_shared" in d else d.get("isImageShared", False),
        "isPoll":        d.get("is_poll")         if "is_poll"         in d else d.get("isPoll",        False),
        "isLoanRequest": d.get("is_loan_request") if "is_loan_request" in d else d.get("isLoanRequest", False),
        "money":         d.get("money",   ""),
        "image":         d.get("image",   ""),
        "caption":       d.get("caption", ""),
        "timestamp":     timestamp,
    }


# ── Products ──────────────────────────────────────────────────────────────────

def product_to_dict(doc_id, d):
    desc         = d.get("description", "") or d.get("desc", "")
    buying_price = float(d.get("buying_price",   0) or d.get("buyingPrice",   0) or 0)
    unit_price   = float(d.get("unit_price",     0) or d.get("unitPrice",     0) or 0)
    avail_stock  = int  (d.get("available_stock",0) or d.get("availableStock",0) or 0)
    reorder_lvl  = int  (d.get("reorder_level", 10) or d.get("reorderLevel", 10) or 10)
    meas_unit    =       d.get("measuring_unit","pcs") or d.get("measuringUnit","pcs") or "pcs"
    category     =       d.get("category",      "")  or d.get("categoryId",   "")
    date_val     =       d.get("created_at")         or d.get("date")    or _now_ms()
    wholesale    = float(d.get("wholesale_price",0)  or d.get("wholesalePrice",0) or 0)
    special      = float(d.get("special_price",  0)  or d.get("specialPrice",   0) or 0)
    tax_rate     = float(d.get("tax_rate",    16.0)  or d.get("taxRate",     16.0) or 16.0)
    is_active    = d.get("is_active", True) if "is_active" in d else d.get("isActive", True)
    return {
        "id":              doc_id,
        "name":            d.get("name", ""),
        "desc":            desc,
        "image":           d.get("image", ""),
        "buying_price":    buying_price,
        "unit_price":      unit_price,
        "available_stock": avail_stock,
        "reorder_level":   reorder_lvl,
        "measuring_unit":  meas_unit,
        "unit":            meas_unit,
        "category":        category,
        "date":            date_val,
        "barcode":         d.get("barcode", ""),
        "code":            d.get("code", ""),
        "wholesale_price": wholesale,
        "special_price":   special,
        "tax_rate":        tax_rate,
        "isActive":        is_active,
    }


# ── Stock ─────────────────────────────────────────────────────────────────────

def stock_in_to_dict(doc_id, d):
    return {
        "id": doc_id,
        "product_id": d.get("product_id", ""),
        "name": d.get("name", ""),
        "measuring_unit": d.get("measuring_unit", "pcs"),
        "buying_price": d.get("buying_price", 0.0),
        "unit_price": d.get("unit_price", 0.0),
        "quantity": d.get("quantity", 0),
        "unit": 1,
        "sold_amount": d.get("sold_amount", 0.0),
        "total_available": d.get("total_available", 0),
        "date": d.get("date", _now_ms()),
    }


def stock_out_to_dict(doc_id, d):
    return {
        "id": doc_id,
        "product_id": d.get("product_id", ""),
        "name": d.get("name", ""),
        "measuring_unit": d.get("measuring_unit", "pcs"),
        "buying_price": d.get("buying_price", 0.0),
        "unit_price": d.get("unit_price", 0.0),
        "quantity": d.get("quantity", 0),
        "unit": 1,
        "date": d.get("date", _now_ms()),
    }


# ── Sales ─────────────────────────────────────────────────────────────────────

def sale_to_dict(doc_id, d):
    product_name   = d.get("product_name", "")   or d.get("name", "")
    person_name    = d.get("person_name",  "")   or d.get("personName", "Walk-in Customer") or "Walk-in Customer"
    customer_id    = d.get("customer_id",  "")   or d.get("customerId", "")
    unit_price     = float(d.get("unit_price", 0) or d.get("unitPrice", 0) or 0)
    buying_price   = float(d.get("buying_price", 0) or d.get("buyingPrice", 0) or 0)
    payment_status = d.get("payment_status", True) if "payment_status" in d else d.get("paymentStatus", True)
    is_credit      = d.get("is_credit",    False) if "is_credit"    in d else d.get("isCredit",    False)
    return {
        "id":            doc_id,
        "product_id":    d.get("product_id", "") or d.get("productId", ""),
        "name":          product_name,
        "unit_price":    unit_price,
        "buying_price":  buying_price,
        "costPrice":     buying_price,
        "quantity":      int(d.get("quantity", 1) or 1),
        "personName":    person_name,
        "customerId":    customer_id,
        "paymentStatus": payment_status,
        "isCredit":      is_credit,
        "date":          d.get("date", _now_ms()),
    }


# ── Customers ─────────────────────────────────────────────────────────────────

def customer_to_dict(doc_id, d):
    balance      = float(d.get("balance",      0)    or d.get("totalDebt",     0)    or 0)
    credit_limit = float(d.get("credit_limit", 0)    or d.get("creditLimit",   0)    or 0)
    is_active    = d.get("is_active", True) if "is_active" in d else d.get("isActive", True)
    group_id     = d.get("group_id", "") or d.get("chatID", "")
    created_at   = d.get("created_at") or d.get("registrationDate") or _now_ms()
    created_by   = d.get("created_by", "") or d.get("createdBy", "")
    return {
        "id":               doc_id,
        "name":             d.get("name", ""),
        "phone":            d.get("phone", ""),
        "email":            d.get("email", ""),
        "address":          d.get("address", ""),
        "balance":          balance,
        "totalDebt":        balance,
        "creditLimit":      credit_limit,
        "notes":            d.get("notes", ""),
        "isActive":         is_active,
        "chatID":           group_id,
        "registrationDate": created_at,
        "createdBy":        created_by,
        "taxId":            d.get("tax_id", "")         or d.get("taxId",         ""),
        "secondaryPhone":   d.get("secondary_phone", "") or d.get("secondaryPhone", ""),
        "category":         d.get("category", ""),
    }


def customer_payment_to_dict(doc_id, d):
    return {
        "id": doc_id,
        "customerId": d.get("customer_id", ""),
        "amount": d.get("amount", 0.0),
        "method": d.get("method", "cash"),
        "notes": d.get("notes", ""),
        "createdBy": d.get("created_by", ""),
        "isAllocated": d.get("is_allocated", True),
        "timestamp": d.get("timestamp", _now_ms()),
    }


# ── Expenses / Income ─────────────────────────────────────────────────────────

def expense_to_dict(doc_id, d):
    is_expense   = d.get("is_expense", True) if "is_expense" in d else d.get("isExpense", True)
    group_id     = d.get("group_id", "") or d.get("chatID", "")
    created_by   = d.get("created_by", "") or d.get("createdBy", "")
    created_at   = d.get("created_at", "") or d.get("createdAt", "")
    pay_method   = d.get("payment_method", "") or d.get("paymentMethod", "") or "cash"
    return {
        "id":            doc_id,
        "chatID":        group_id,
        "name":          d.get("name", ""),
        "price":         float(d.get("price", 0) or 0),
        "isExpense":     is_expense,
        "category":      d.get("category", "Other"),
        "paymentMethod": pay_method,
        "notes":         d.get("notes", ""),
        "createdBy":     created_by,
        "timestamp":     d.get("timestamp", _now_ms()),
        "createdAt":     created_at,
    }


# ── Licenses ──────────────────────────────────────────────────────────────────

def license_to_dict(doc_id, d):
    return {
        "id": doc_id,
        "key": d.get("key", ""),
        "companyName":    d.get("company_name", "")    or d.get("companyName", ""),
        "issuedAt":       d.get("issued_at",    "")    or d.get("issuedAt",    ""),
        "expiresAt":      d.get("expires_at",   "")    or d.get("expiresAt",   ""),
        "isUsed":         d.get("is_used",       False) or d.get("isUsed",      False),
        "assignedToUid":  d.get("assigned_to_uid", "")  or d.get("assignedToUid",  ""),
        "recipientEmail": d.get("recipient_email", "")  or d.get("recipientEmail", ""),
        "usedAt":         d.get("used_at",       "")   or d.get("usedAt",       ""),
    }


