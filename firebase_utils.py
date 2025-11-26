from firebase_admin import firestore, get_app

from config import FIREBASE_CREDENTIALS_PATH


def get_db():
    """
    Return a Firestore client for the default Firebase app.

    If the app has not been initialized yet (e.g. when running a script),
    it will be initialized using the service account credentials.
    """
    try:
        # Will raise ValueError if no default app exists
        get_app()
    except ValueError:
        from firebase_admin import credentials, initialize_app

        cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
        initialize_app(cred)

    return firestore.client()


