from flask import Flask, request, jsonify
from firebase_admin import credentials, messaging, initialize_app

app = Flask(__name__)

# Initialize Firebase Admin SDK
cred = credentials.Certificate('serviceAccountKey.json')
initialize_app(cred)

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
        return jsonify({'error': 'Failed to send notification', 'details': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)