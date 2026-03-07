#!/usr/bin/env python
"""
Single entry point to run all verification crons systematically.

Usage:
    python run_all_crons.py
"""

import logging
import time
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


def run_all_crons():
    """Run all verification crons once and exit."""
    start_time = time.time()
    
    logger.info("════════════════════════════════════════════════════════════════")
    logger.info("RUNNING ALL CRONS - %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("════════════════════════════════════════════════════════════════")

    # Phase 1: Proactive Payment Resolution (Active Status Query)
    logger.info("")
    logger.info("── PHASE 1: PROACTIVE PAYMENT RESOLUTION ──")
    try:
        from payment_resolver import PaymentResolverService
        resolver = PaymentResolverService()
        resolver.resolve_now()
    except Exception as e:
        logger.error("Proactive payment resolution failed: %s", e)

    # Phase 2: Payment (STK) verification
    logger.info("")
    logger.info("── PHASE 2: PAYMENT VERIFICATION ──")
    try:
        from auto_verify import main as run_auto_verify
        run_auto_verify()
    except Exception as e:
        logger.error("Payment verification failed: %s", e)

    # Phase 3: Withdrawal verification
    logger.info("")
    logger.info("── PHASE 3: WITHDRAWAL VERIFICATION ──")
    try:
        from verify_withdrawals import main as run_withdrawals
        run_withdrawals()
    except Exception as e:
        logger.error("Withdrawal verification failed: %s", e)

    # Phase 4: Paybill verification
    logger.info("")
    logger.info("── PHASE 4: PAYBILL VERIFICATION ──")
    try:
        from verify_paybill import main as run_paybill
        run_paybill()
    except Exception as e:
        logger.error("Paybill verification failed: %s", e)

    elapsed = time.time() - start_time
    logger.info("")
    logger.info("════════════════════════════════════════════════════════════════")
    logger.info("ALL CRONS COMPLETED in %.2f seconds", elapsed)
    logger.info("════════════════════════════════════════════════════════════════")


if __name__ == "__main__":
    try:
        run_all_crons()
    except KeyboardInterrupt:
        logger.info("Cron service stopped by user.")
    except Exception as e:
        logger.critical("Cron service crashed: %s", e)
