import logging
from datetime import datetime

from firebase_utils import get_db
from payment_verification_service import PaymentVerificationService


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> None:
    print(f"Starting automatic payment verification: {datetime.now():%Y-%m-%d %H:%M:%S}")

    db = get_db()
    transactions_collection = db.collection("TRANSACTIONS")

    user_docs = list(transactions_collection.stream())
    user_ids = [doc.id for doc in user_docs]

    if not user_ids:
        print("No users found in TRANSACTIONS collection")
        return

    print(f"Found {len(user_ids)} users to process")

    service = PaymentVerificationService()
    total_verified = 0

    for user_id in user_ids:
        print(f"Processing user: {user_id}")
        result = service.process_pending_transactions(user_id)

        if result.get("success"):
            count = int(result.get("verifiedCount", 0))
            print(f"  Verified {count} transactions")
            total_verified += count

            for tx in result.get("results", []):
                print(f"    - Transaction: {tx.get('transactionId')}")
                print(f"      Code: {tx.get('transactionCode')}")
                print(f"      Amount: KES {tx.get('amount')}")
                print(f"      New Balance: KES {tx.get('newBalance')}")
        else:
            print(f"  Error: {result.get('error', 'Unknown')}")

    print(f"\nCompleted: {total_verified} total transactions verified")
    print(f"Finished: {datetime.now():%Y-%m-%d %H:%M:%S}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}")
        logging.exception("Cron error")


