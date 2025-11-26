#!/usr/bin/env python
"""
Python port of PHP cron/paybill_verify.php

Run periodically to process PAYBILL transactions for all users.
"""

import logging

from firebase_utils import get_db
from paybill_verification_service import PaybillVerificationService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("════════════════════════════════════════════════════════════════")
    logger.info("PAYBILL VERIFICATION STARTED")
    logger.info("════════════════════════════════════════════════════════════════")

    db = get_db()

    # Get all users from USER collection
    users_ref = db.collection("USER")
    all_users = {doc.id: doc.to_dict() for doc in users_ref.stream()}

    if not all_users:
        logger.error("No users found")
        return

    logger.info("Found %d users", len(all_users))

    service = PaybillVerificationService()

    # Fetch payments ONCE for all users
    logger.info("Fetching payments from M-Pesa API...")
    payments = service._fetch_payments_from_api()
    if not payments:
        logger.error("Failed to fetch payment data from API")
        return
    logger.info("Fetched %d payments", len(payments))

    total_added = 0

    for user_id, user_data in all_users.items():
        account_number = (user_data or {}).get("accountNumber", "")
        if not account_number:
            continue

        logger.info("User %s (Account: %s)...", user_id, account_number)

        result = service.process_paybill_for_user(user_id, account_number, payments=payments)

        if result.get("success") and result.get("addedCount", 0) > 0:
            count = result["addedCount"]
            logger.info("  %d transactions added", count)
            total_added += count
            for tx in result.get("results", []):
                logger.info(
                    "    └─ [%s] Amount: KES %s",
                    tx.get("transactionCode", "N/A"),
                    tx.get("amount", 0),
                )
        else:
            logger.info("  no new transactions")

    logger.info("════════════════════════════════════════════════════════════════")
    logger.info("Total added: %d", total_added)
    logger.info("════════════════════════════════════════════════════════════════")

    PaybillVerificationService.cleanup_cache()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logger.exception("ERROR: %s", exc)

