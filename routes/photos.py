"""
Generic photo upload proxy for the Flutter app.

Mirrors the Android PhotoUploadService: uploads go to the media server
(api.kit-ifms.com) with the media API key, tagged with an upload_type
(profile | product | group | chat) and the associated record id. Returns the
public URL for the caller to store on the relevant Firestore document via the
normal update endpoints.
"""
import logging

import requests
from flask import Blueprint, request, jsonify

from auth_utils import require_auth, get_jwt_identity
from config import MEDIA_UPLOAD_API_KEY
from firebase_utils import get_db
import db_constants as C

photos_bp = Blueprint("photos", __name__, url_prefix="/photos")

_MEDIA_UPLOAD_URL = "https://api.kit-ifms.com/api/photos/upload"
_MAX_BYTES = 10 * 1024 * 1024
_ALLOWED_TYPES = {"image/jpeg", "image/png", "image/webp"}
_ALLOWED_UPLOAD_TYPES = {"profile", "product", "group", "chat"}


@photos_bp.route("/upload", methods=["POST"])
@require_auth
def upload_photo():
    uid = get_jwt_identity()

    uploaded = request.files.get("file") or request.files.get("image")
    if uploaded is None or not uploaded.filename:
        return jsonify({"error": "file is required"}), 400

    upload_type = (request.form.get("upload_type") or "").strip().lower()
    if upload_type not in _ALLOWED_UPLOAD_TYPES:
        return jsonify({"error": "Invalid upload_type"}), 400
    associated_id = (request.form.get("associated_id") or "").strip() or uid

    content_type = (uploaded.mimetype or "").lower()
    if content_type in {"", "application/octet-stream"}:
        ext = uploaded.filename.rsplit(".", 1)[-1].lower() if "." in uploaded.filename else ""
        content_type = {
            "jpg": "image/jpeg", "jpeg": "image/jpeg",
            "png": "image/png", "webp": "image/webp",
        }.get(ext, content_type)
    if content_type not in _ALLOWED_TYPES:
        return jsonify({"error": "Unsupported image type"}), 415

    payload = uploaded.read(_MAX_BYTES + 1)
    if not payload:
        return jsonify({"error": "File is empty"}), 400
    if len(payload) > _MAX_BYTES:
        return jsonify({"error": "File exceeds 10 MB"}), 413

    try:
        upstream = requests.post(
            _MEDIA_UPLOAD_URL,
            headers={
                "X-API-KEY": MEDIA_UPLOAD_API_KEY,
                "Accept": "application/json",
                # The media server blocks the default python-requests user
                # agent via mod_security; mimic the Android client.
                "User-Agent": "okhttp/4.12.0",
            },
            files={"image": (uploaded.filename, payload, content_type)},
            data={"upload_type": upload_type, "associated_id": associated_id},
            timeout=180,
        )
    except Exception as exc:
        logging.exception("Photo upload failed (%s/%s): %s", upload_type, associated_id, exc)
        return jsonify({"error": "Media upload service unavailable"}), 502
    try:
        data = upstream.json()
    except ValueError:
        logging.error("Media server returned non-JSON (%s): %s",
                      upstream.status_code, upstream.text[:300])
        return jsonify({"error": f"Media server error ({upstream.status_code})"}), 502
    if not upstream.ok or data.get("success") is not True:
        return jsonify({"error": data.get("error", "Media upload failed")}), 502

    url = str(data.get("url", ""))
    if url and not url.startswith(("http://", "https://")):
        url = "https://api.kit-ifms.com/" + url.lstrip("/")

    # Profile uploads update the user document directly, same as Android.
    if upload_type == "profile":
        try:
            # Android's UserModel reads "image"/"photoUrl"; this backend and
            # the Flutter app read "image_url" — keep all three in sync.
            get_db().collection(C.USERS).document(uid).update({
                "image_url": url,
                "image": url,
                "photoUrl": url,
            })
        except Exception as exc:
            logging.warning("Could not update profile image for %s: %s", uid, exc)

    return jsonify({"url": url, "mediaId": data.get("photo_id")}), 201
