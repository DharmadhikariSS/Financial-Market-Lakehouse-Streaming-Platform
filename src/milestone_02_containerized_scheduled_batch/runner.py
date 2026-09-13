"""Production Batch Runner for unattended execution."""

import sys
from pathlib import Path

from src.common.config import BASE_DIR, settings
from src.common.logger import get_logger
from src.milestone_01_baseline_scripted_ingestion.pipeline import IngestionPipeline

logger = get_logger("milestone_02.runner")


def run_batch(input_path: Path | None = None) -> int:
    """Executes a single scheduled batch run."""
    logger.info("==================================================")
    logger.info("STARTING SCHEDULED BATCH EXECUTION")
    logger.info(f"Target DB Engine: {settings.DB_ENGINE}")
    logger.info("==================================================")

    target_file = input_path or (BASE_DIR / "data" / "unit_test_50.csv")

    if not target_file.exists():
        logger.error(f"Batch payload not found at: {target_file}")
        return 1

    try:
        pipeline = IngestionPipeline(target_engine=settings.DB_ENGINE)
        pipeline.initialize_schema()

        metrics = pipeline.process_csv(target_file, chunk_size=settings.BATCH_SIZE)

        logger.info("Batch execution succeeded.")
        logger.info(f"Audit Metrics: {metrics}")
        return 0

    except Exception as e:
        logger.exception(f"Fatal error during batch execution: {e}")
        return 1


if __name__ == "__main__":
    file_arg = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    exit_code = run_batch(file_arg)
    sys.exit(exit_code)
