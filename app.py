from flask import Flask, jsonify, request
from firebase_admin import credentials, initialize_app, messaging

from config import API_SECRET_TOKEN, FIREBASE_CREDENTIALS_PATH, API_KEY
from routes.auth import auth_bp
from routes.users import users_bp
from routes.groups import groups_bp
from routes.products import products_bp
from routes.sales import sales_bp
from routes.customers import customers_bp
from routes.stock import stock_bp
from routes.messages import messages_bp
from routes.group_accounts import group_accounts_bp
from routes.direct_messages import direct_messages_bp
from routes.licenses import licenses_bp
from routes.notifications import notifications_bp
from routes.expenses import expenses_bp, income_bp
from routes.reports import reports_bp
from routes.mpesa import mpesa_bp
from routes.dashboard import dashboard_bp

app = Flask(__name__)
app.config["API_KEY"] = API_KEY

# Initialize Firebase Admin SDK using the same path as config.py
cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
initialize_app(cred)


def _add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
    return response


def _validate_auth(auth_header: str) -> bool:
    """
    Simple Bearer token auth, mirroring the PHP validateAuth() logic.
    """
    if not auth_header:
        return False
    token = auth_header.replace('Bearer ', '').strip()
    # Constant-time comparison to avoid timing attacks
    import hmac

    return hmac.compare_digest(API_SECRET_TOKEN, token)

@app.route('/send-notification', methods=['POST'])
def send_notification():
    try:
        # Parse JSON body
        data = request.get_json()
        if not data or 'token' not in data or 'title' not in data or 'body' not in data:
            return jsonify({'error': 'Missing required fields: token, title, body'}), 400

        token = data['token']
        title = data['title']
        body = data['body']

        # Create notification message
        message = messaging.Message(
            notification=messaging.Notification(
                title=title,
                body=body
            ),
            token=token
        )

        # Send notification
        response = messaging.send(message)
        print(f'Successfully sent message: {response}')
        return jsonify({'success': True, 'message_id': response}), 200

    except Exception as e:
        print(f'Error sending message: {e}')
        return (
            jsonify(
                {'error': 'Failed to send notification', 'details': str(e)},
            ),
            500,
        )


# ── kit-ifms CRUD & report blueprints ────────────────────────────────────────
app.register_blueprint(auth_bp)
app.register_blueprint(users_bp)
app.register_blueprint(groups_bp)
app.register_blueprint(products_bp)
app.register_blueprint(sales_bp)
app.register_blueprint(customers_bp)
app.register_blueprint(stock_bp)
app.register_blueprint(messages_bp)
app.register_blueprint(group_accounts_bp)
app.register_blueprint(direct_messages_bp)
app.register_blueprint(licenses_bp)
app.register_blueprint(notifications_bp)
app.register_blueprint(expenses_bp)
app.register_blueprint(income_bp)
app.register_blueprint(reports_bp)
app.register_blueprint(mpesa_bp)
app.register_blueprint(dashboard_bp)


@app.after_request
def _cors(response):
    """Allow the Flutter app (and browser tooling) to call all endpoints."""
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = (
        "Content-Type, Authorization, X-API-Key"
    )
    return response


@app.route("/auth/register", methods=["OPTIONS"])
@app.route("/auth/login", methods=["OPTIONS"])
@app.route("/auth/refresh", methods=["OPTIONS"])
@app.route("/auth/me", methods=["OPTIONS"])
@app.route("/auth/reset-password", methods=["OPTIONS"])
@app.route("/users/<uid>", methods=["OPTIONS"])
@app.route("/users/<uid>/fcm-token", methods=["OPTIONS"])
@app.route("/groups", methods=["OPTIONS"])
@app.route("/groups/<group_id>", methods=["OPTIONS"])
@app.route("/groups/<group_id>/members/<member_id>/role", methods=["OPTIONS"])
@app.route("/groups/<group_id>/products", methods=["OPTIONS"])
@app.route("/groups/<group_id>/products/<product_id>", methods=["OPTIONS"])
@app.route("/groups/<group_id>/products/<product_id>/adjust-stock", methods=["OPTIONS"])
@app.route("/groups/<group_id>/sales", methods=["OPTIONS"])
@app.route("/groups/<group_id>/sales/<sale_id>/mark-paid", methods=["OPTIONS"])
@app.route("/groups/<group_id>/customers", methods=["OPTIONS"])
@app.route("/groups/<group_id>/customers/<customer_id>", methods=["OPTIONS"])
@app.route("/groups/<group_id>/customers/<customer_id>/payments", methods=["OPTIONS"])
@app.route("/groups/<group_id>/stock/in", methods=["OPTIONS"])
@app.route("/groups/<group_id>/stock/out", methods=["OPTIONS"])
@app.route("/groups/<group_id>/members", methods=["OPTIONS"])
@app.route("/groups/<group_id>/members/<member_id>", methods=["OPTIONS"])
@app.route("/groups/<group_id>/settings", methods=["OPTIONS"])
@app.route("/groups/<group_id>/accounts", methods=["OPTIONS"])
@app.route("/groups/<group_id>/accounts/<account_id>/transactions", methods=["OPTIONS"])
@app.route("/groups/<group_id>/accounts/<account_id>/deposit", methods=["OPTIONS"])
@app.route("/messages/direct/<other_user_id>", methods=["OPTIONS"])
@app.route("/messages/direct/<other_user_id>/read", methods=["OPTIONS"])
@app.route("/chats", methods=["OPTIONS"])
@app.route("/groups/<group_id>/messages", methods=["OPTIONS"])
@app.route("/groups/<group_id>/messages/media", methods=["OPTIONS"])
@app.route("/groups/<group_id>/messages/<message_id>/react", methods=["OPTIONS"])
@app.route("/groups/<group_id>/messages/<message_id>/poll/vote", methods=["OPTIONS"])
@app.route("/groups/<group_id>/messages/<message_id>/loan-action", methods=["OPTIONS"])
@app.route("/groups/<group_id>/messages/<message_id>/guarantor-action", methods=["OPTIONS"])
@app.route("/groups/<group_id>/expenses", methods=["OPTIONS"])
@app.route("/groups/<group_id>/expenses/<entry_id>", methods=["OPTIONS"])
@app.route("/groups/<group_id>/income", methods=["OPTIONS"])
@app.route("/groups/<group_id>/income/<entry_id>", methods=["OPTIONS"])
@app.route("/groups/<group_id>/reports/sales", methods=["OPTIONS"])
@app.route("/groups/<group_id>/reports/stock", methods=["OPTIONS"])
@app.route("/groups/<group_id>/reports/expenses", methods=["OPTIONS"])
@app.route("/groups/<group_id>/reports/income", methods=["OPTIONS"])
@app.route("/licenses", methods=["OPTIONS"])
@app.route("/licenses/<license_id>", methods=["OPTIONS"])
@app.route("/licenses/verify", methods=["OPTIONS"])
@app.route("/licenses/my-license", methods=["OPTIONS"])
@app.route("/licenses/admin/check", methods=["OPTIONS"])
@app.route("/licenses/<license_id>/use", methods=["OPTIONS"])
@app.route("/licenses/<license_id>/send-email", methods=["OPTIONS"])
@app.route("/notifications/send", methods=["OPTIONS"])
@app.route("/mpesa/stk-push", methods=["OPTIONS"])
@app.route("/mpesa/stk-query", methods=["OPTIONS"])
@app.route("/mpesa/stk-callback", methods=["OPTIONS"])
def _preflight(**_):
    """Handle CORS preflight for all kit-ifms routes."""
    return "", 204


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
