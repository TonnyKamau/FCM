from flask import Flask, jsonify, request
from firebase_admin import credentials, initialize_app, messaging

from config import API_SECRET_TOKEN, FIREBASE_CREDENTIALS_PATH, API_KEY
from payment_verification_service import PaymentVerificationService
from withdrawal_verification_service import WithdrawalVerificationService
from paybill_verification_service import PaybillVerificationService
from routes.auth import auth_bp
from routes.users import users_bp
from routes.groups import groups_bp
from routes.products import products_bp
from routes.sales import sales_bp
from routes.customers import customers_bp
from routes.stock import stock_bp
from routes.messages import messages_bp
from routes.licenses import licenses_bp
from routes.notifications import notifications_bp
from routes.expenses import expenses_bp, income_bp
from routes.reports import reports_bp

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


@app.route('/payment-verification', methods=['POST', 'OPTIONS'])
def payment_verification():
    """
    Flask port of the original PHP endpoint.php.

    Expects JSON body with an "action" field:
      - verify_payment
      - verify_all_pending
    """
    if request.method == 'OPTIONS':
        # Preflight CORS
        resp = jsonify({'ok': True})
        return _add_cors_headers(resp), 200

    # Auth header
    auth_header = request.headers.get('Authorization') or ''
    if not _validate_auth(auth_header):
        resp = jsonify({'error': 'Unauthorized - Invalid or missing token'})
        return _add_cors_headers(resp), 401

    # JSON body
    data = request.get_json(silent=True) or {}
    action = data.get('action', '')

    payment_service = PaymentVerificationService()

    try:
        if action == 'verify_payment':
            merchant_request_id = data.get('merchantRequestId', '')
            user_id = data.get('userId', '')
            account_type = data.get('accountType', 'normal')
            current_balance = float(data.get('currentBalance', 0) or 0)
            expected_amount = float(data.get('expectedAmount', 0) or 0)

            if not merchant_request_id or not user_id:
                resp = jsonify(
                    {'error': 'Missing merchantRequestId or userId'},
                )
                return _add_cors_headers(resp), 400

            result = payment_service.verify_and_update_balance(
                merchant_request_id,
                user_id,
                account_type,
                current_balance,
                expected_amount,
            )
            resp = jsonify(result)
            return _add_cors_headers(resp), 200

        if action == 'verify_withdrawal':
            withdrawal_service = WithdrawalVerificationService()

            account_reference = data.get('accountReference', '')
            user_id = data.get('userId', '')
            account_type = data.get('accountType', 'NORMAL')
            withdrawal_amount = float(data.get('withdrawalAmount', 0) or 0)
            is_group_withdrawal = bool(data.get('isGroupWithdrawal', False))
            group_id = data.get('groupId')

            if not account_reference or not user_id:
                resp = jsonify(
                    {'error': 'Missing accountReference or userId'},
                )
                return _add_cors_headers(resp), 400

            result = withdrawal_service.verify_specific_withdrawal(
                user_id=user_id,
                account_reference=account_reference,
                account_type=account_type,
                withdrawal_amount=withdrawal_amount,
                is_group_withdrawal=is_group_withdrawal,
                group_id=group_id,
            )
            resp = jsonify(result)
            return _add_cors_headers(resp), 200

        if action == 'verify_all_pending':
            user_id = data.get('userId', '')
            if not user_id:
                resp = jsonify({'error': 'userId required'})
                return _add_cors_headers(resp), 400

            result = payment_service.process_pending_transactions(user_id)
            resp = jsonify(result)
            return _add_cors_headers(resp), 200

        resp = jsonify(
            {
                'error': (
                    'Invalid action. Use: verify_payment, '
                    'verify_withdrawal, or verify_all_pending'
                ),
            },
        )
        return _add_cors_headers(resp), 400
    except Exception as exc:  # noqa: BLE001
        app.logger.exception('Payment verification API error: %s', exc)
        resp = jsonify({'error': 'Internal server error'})
        return _add_cors_headers(resp), 500


@app.route('/paybill-verification', methods=['POST', 'OPTIONS'])
def paybill_verification():
    """
    Endpoint for Android to trigger PAYBILL verification for a user.
    Expects JSON body with userId and accountNumber.
    """
    if request.method == 'OPTIONS':
        resp = jsonify({'ok': True})
        return _add_cors_headers(resp), 200

    auth_header = request.headers.get('Authorization') or ''
    if not _validate_auth(auth_header):
        resp = jsonify({'error': 'Unauthorized - Invalid or missing token'})
        return _add_cors_headers(resp), 401

    data = request.get_json(silent=True) or {}
    user_id = data.get('userId', '')
    account_number = data.get('accountNumber', '')

    if not user_id or not account_number:
        resp = jsonify({'error': 'Missing userId or accountNumber'})
        return _add_cors_headers(resp), 400

    try:
        paybill_service = PaybillVerificationService()
        result = paybill_service.process_paybill_for_user(user_id, account_number)
        resp = jsonify(result)
        return _add_cors_headers(resp), 200
    except Exception as exc:
        app.logger.exception('Paybill verification error: %s', exc)
        resp = jsonify({'error': 'Internal server error'})
        return _add_cors_headers(resp), 500

# ── kit-ifms CRUD & report blueprints ────────────────────────────────────────
app.register_blueprint(auth_bp)
app.register_blueprint(users_bp)
app.register_blueprint(groups_bp)
app.register_blueprint(products_bp)
app.register_blueprint(sales_bp)
app.register_blueprint(customers_bp)
app.register_blueprint(stock_bp)
app.register_blueprint(messages_bp)
app.register_blueprint(licenses_bp)
app.register_blueprint(notifications_bp)
app.register_blueprint(expenses_bp)
app.register_blueprint(income_bp)
app.register_blueprint(reports_bp)


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
@app.route("/groups/<group_id>/messages", methods=["OPTIONS"])
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
def _preflight(**_):
    """Handle CORS preflight for all kit-ifms routes."""
    return "", 204


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)