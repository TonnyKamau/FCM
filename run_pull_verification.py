import logging
import time
from datetime import datetime

from pull_verification_service import PullVerificationService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting Pull Verification Service...")
    service = PullVerificationService()

    while True:
        try:
            logger.info("Checking for pending transactions via Pull API...")
            result = service.process_all_pending()
            
            if result.get("success"):
                count = result.get("verifiedCount", 0)
                if count > 0:
                    logger.info(f"Verified {count} transactions.")
                    for res in result.get("results", []):
                        logger.info(f"  - User: {res['userId']}, Trans: {res['transactionId']}, Amount: {res['amount']}, Code: {res['transactionCode']}")
                else:
                    logger.info("No matching transactions found.")
            else:
                logger.error(f"Error during verification: {result.get('error')}")

        except Exception as e:
            logger.exception(f"Unexpected error in main loop: {e}")

        # Wait before next poll (e.g., 5 minutes)
        logger.info("Waiting 5 minutes for next poll...")
        time.sleep(300)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Service stopped by user.")
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
