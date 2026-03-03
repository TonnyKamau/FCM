from flask import Blueprint, request, jsonify
from auth_utils import require_auth
from utils.notification_utils import get_user_fcm_tokens, send_notification_to_tokens

notifications_bp = Blueprint("notifications", __name__, url_prefix="/notifications")


@notifications_bp.route("/send", methods=["POST"])
@require_auth
def send_notification():
    """
    Flutter sends:
      {
        "userIds": ["uid1", "uid2"],
        "title":   "Hello",
        "body":    "You have a new message",
        "data":    { "chat_id": "...", "type": "chat_message", ... }
      }

    Backend resolves each userId → currentFCMToken from Firestore,
    then delivers via firebase_admin.messaging (high-priority).
    """
    data     = request.get_json() or {}
    user_ids = data.get("userIds", [])
    title    = data.get("title", "").strip()
    body_txt = data.get("body", "").strip()
    extra    = data.get("data", {})

    if not user_ids or not title or not body_txt:
        return jsonify({"error": "userIds, title and body are required"}), 400

    tokens = get_user_fcm_tokens(user_ids)
    if not tokens:
        return jsonify({"message": "No FCM tokens found for given users", "success": 0, "failure": 0})

    result = send_notification_to_tokens(
        tokens=tokens,
        title=title,
        body=body_txt,
        data={
            "timestamp": str(int(__import__("time").time() * 1000)),
            "click_action": "FLUTTER_NOTIFICATION_CLICK",
            **extra,
        },
    )
    return jsonify(result)
