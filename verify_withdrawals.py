#!/usr/bin/env python
"""
Python port of PHP cron/verify_withdrawals.php

Run periodically (e.g. every 5 minutes) to:
1. Verify pending B2C withdrawals against M-Pesa API
2. Handle timed-out withdrawals (>30 min in PROCESSING)
"""

import logging
import time
from datetime import datetime

from firebase_utils import get_db
from withdrawal_verification_service import WithdrawalVerificationService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("════════════════════════════════════════════════════════════════")
    logger.info("WITHDRAWAL VERIFICATION CRON JOB STARTED")
    logger.info("════════════════════════════════════════════════════════════════")

    db = get_db()

    # Get all users from USER collection
    users_ref = db.collection("USER")
    all_users = {doc.id: doc.to_dict() for doc in users_ref.stream()}

    if not all_users:
        logger.error("No users found")
        return

    logger.info("Found %d users to process", len(all_users))

    service = WithdrawalVerificationService()

    # Fetch payments ONCE (big speed improvement)
    logger.info("Fetching payments from M-Pesa API...")
    payments = service._fetch_payments_from_api()
    if not payments:
        logger.error("Failed to fetch payment data from API")
        return
    logger.info("Fetched %d payments", len(payments))

    total_verified = 0
    total_timed_out = 0
    processed_users = 0
    users_with_withdrawals = 0

    logger.info("──────────────────────────────────────────────────────────────")
    logger.info("PHASE 1: WITHDRAWAL VERIFICATION")
    logger.info("──────────────────────────────────────────────────────────────")

    for user_id, user_data in all_users.items():
        user_name = (user_data or {}).get("name", "Unknown")
        account_number = (user_data or {}).get("accountNumber", "N/A")
        logger.info("Processing user: %s (ID: %s, Account: %s)", user_name, user_id, account_number)

        result = service.process_withdrawals_for_user(user_id, payments=payments)

        if result.get("success"):
            processed_users += 1
            count = result.get("processedCount", 0)
            if count > 0:
                users_with_withdrawals += 1
                total_verified += count
                logger.info("  ✓ VERIFIED: %d withdrawal(s)", count)
                for tx in result.get("results", []):
                    logger.info(
                        "    └─ [%s] Amount: KES %s | ID: %s",
                        tx.get("transactionCode", "N/A"),
                        tx.get("amount", 0),
                        tx.get("transactionId", "N/A"),
                    )
            else:
                logger.info("  • No pending withdrawals")
        else:
            logger.error("  ✗ ERROR: %s", result.get("error"))

    logger.info("──────────────────────────────────────────────────────────────")
    logger.info("PHASE 2: TIMEOUT CHECK")
    logger.info("──────────────────────────────────────────────────────────────")

    for user_id, user_data in all_users.items():
        user_name = (user_data or {}).get("name", "Unknown")
        timeout_result = service.check_for_timed_out_withdrawals_for_user(user_id)
        if timeout_result.get("success") and timeout_result.get("timedOutCount", 0) > 0:
            logger.info(
                "  ⚠ [%s] TIMEOUT: %d withdrawal(s) timed out and reversed",
                user_name,
                timeout_result["timedOutCount"],
            )
            total_timed_out += timeout_result["timedOutCount"]

    if total_timed_out == 0:
        logger.info("  ✓ No timed out withdrawals found")

    logger.info("════════════════════════════════════════════════════════════════")
    logger.info("WITHDRAWAL VERIFICATION SUMMARY")
    logger.info("════════════════════════════════════════════════════════════════")
    logger.info("Total Users Processed:          %d", processed_users)
    logger.info("Users with Withdrawals:         %d", users_with_withdrawals)
    logger.info("Withdrawals Verified:           %d", total_verified)
    logger.info("Withdrawals Timed Out:          %d", total_timed_out)
    logger.info("════════════════════════════════════════════════════════════════")

    WithdrawalVerificationService.cleanup_cache()

    logger.info("✓ Withdrawal verification completed successfully")
    logger.info("════════════════════════════════════════════════════════════════")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logger.exception("CRITICAL ERROR: %s", exc)

