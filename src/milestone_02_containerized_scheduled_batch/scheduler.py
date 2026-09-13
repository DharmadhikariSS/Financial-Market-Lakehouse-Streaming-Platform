"""Lightweight Unattended Batch Scheduler.

Runs batch ingestion on a periodic cron-like interval with graceful shutdown signals.
Memory footprint: < 25MB RAM.
"""

import os
import signal
import sys
import time

from src.common.config import BASE_DIR
from src.common.logger import get_logger
from src.milestone_02_containerized_scheduled_batch.runner import run_batch

logger = get_logger("milestone_02.scheduler")

RUNNING = True


def handle_shutdown(signum: int, frame: object) -> None:
    """Handles graceful termination signals."""
    global RUNNING
    logger.info(f"Received signal {signum}. Initiating graceful scheduler shutdown...")
    RUNNING = False


def main() -> None:
    # Register POSIX / Windows signals
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # Configurable interval (seconds)
    interval_seconds = int(os.getenv("SCHEDULE_INTERVAL_SECONDS", "60"))
    logger.info(f"Batch scheduler active. Execution frequency: every {interval_seconds}s.")

    data_file = BASE_DIR / "data" / "unit_test_50.csv"

    iteration = 1
    while RUNNING:
        logger.info(f"Triggering scheduled batch iteration #{iteration}...")
        try:
            exit_code = run_batch(data_file)
            logger.info(f"Iteration #{iteration} completed with exit code: {exit_code}")
        except Exception as e:
            logger.error(f"Iteration #{iteration} failed: {e}")

        iteration += 1

        # Sleep in small increments to respond immediately to SIGINT/SIGTERM
        for _ in range(interval_seconds):
            if not RUNNING:
                break
            time.sleep(1)

    logger.info("Scheduler process terminated cleanly.")
    sys.exit(0)


if __name__ == "__main__":
    main()
