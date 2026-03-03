"""FCM push notification utility using firebase_admin.messaging."""
import logging
from firebase_admin import messaging

log = logging.getLogger(__name__)


def send_notification_to_tokens(tokens, title, body, data=None):
    if not tokens:
        return {"success": 0, "failure": 0}
    str_data = {k: str(v) for k, v in (data or {}).items()}
    messages = [
        messaging.Message(
            notification=messaging.Notification(title=title, body=body),
            data=str_data,
            token=token,
            android=messaging.AndroidConfig(priority="high"),
        )
        for token in tokens
    ]
    try:
        batch_resp = messaging.send_each(messages)
        for i, resp in enumerate(batch_resp.responses):
            if not resp.success:
                log.warning("FCM send failed for token[%d]: %s", i, resp.exception)
        return {"success": batch_resp.success_count, "failure": batch_resp.failure_count}
    except Exception as e:
        log.error("FCM batch send error: %s", e)
        return {"success": 0, "failure": len(tokens)}


def get_user_fcm_tokens(user_ids):
    from firebase_utils import get_db
    import db_constants as C
    db = get_db()
    tokens = []
    for uid in user_ids:
        try:
            doc = db.collection(C.USERS).document(uid).get()
            if doc.exists:
                token = doc.to_dict().get(C.FCM_TOKEN_FIELD, "").strip()
                if token:
                    tokens.append(token)
        except Exception as e:
            log.warning("Failed to fetch FCM token for %s: %s", uid, e)
    return tokens
