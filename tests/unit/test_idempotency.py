"""Unit tests verifying pipeline idempotency and conflict resolution."""

import csv
from pathlib import Path

import pytest

from src.common.config import settings
from src.milestone_01_baseline_scripted_ingestion.pipeline import IngestionPipeline


@pytest.fixture
def temp_duckdb_pipeline(tmp_path: Path) -> IngestionPipeline:
    """Provides an isolated IngestionPipeline instance pointing to a temporary DuckDB file."""
    db_file = tmp_path / "test_ledger.duckdb"
    settings.DUCKDB_PATH = str(db_file)
    pipeline = IngestionPipeline(target_engine="duckdb")
    pipeline.initialize_schema()
    return pipeline


def create_test_csv(file_path: Path, rows: list[dict[str, object]]) -> None:
    """Helper to write sample CSV fixtures."""
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "trade_id",
                "symbol",
                "price",
                "quantity",
                "quote_quantity",
                "trade_timestamp",
                "is_buyer_maker",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def test_idempotent_ingestion(temp_duckdb_pipeline: IngestionPipeline, tmp_path: Path) -> None:
    pipeline = temp_duckdb_pipeline
    csv_file = tmp_path / "batch_1.csv"

    initial_rows: list[dict[str, object]] = [
        {
            "trade_id": 1,
            "symbol": "BTCUSDT",
            "price": 60000.0,
            "quantity": 0.5,
            "quote_quantity": 30000.0,
            "trade_timestamp": "2026-03-01T10:00:00Z",
            "is_buyer_maker": False,
        },
        {
            "trade_id": 2,
            "symbol": "ETHUSDT",
            "price": 3000.0,
            "quantity": 2.0,
            "quote_quantity": 6000.0,
            "trade_timestamp": "2026-03-01T10:01:00Z",
            "is_buyer_maker": True,
        },
    ]
    create_test_csv(csv_file, initial_rows)

    # 1. First Run: Should insert 2 rows
    metrics_1 = pipeline.process_csv(csv_file)
    assert metrics_1["total_valid"] == 2
    assert metrics_1["final_table_count"] == 2

    # 2. Re-run identical file: Table count must remain 2 (Zero duplicate records)
    metrics_2 = pipeline.process_csv(csv_file)
    assert metrics_2["total_valid"] == 2
    assert metrics_2["final_table_count"] == 2

    # 3. Mutation run: Trade #1 has price adjusted to 61000.0
    mutated_rows: list[dict[str, object]] = [
        {
            "trade_id": 1,
            "symbol": "BTCUSDT",
            "price": 61000.0,  # Modified price
            "quantity": 0.5,
            "quote_quantity": 30500.0,
            "trade_timestamp": "2026-03-01T10:00:00Z",
            "is_buyer_maker": False,
        },
        {
            "trade_id": 2,
            "symbol": "ETHUSDT",
            "price": 3000.0,
            "quantity": 2.0,
            "quote_quantity": 6000.0,
            "trade_timestamp": "2026-03-01T10:01:00Z",
            "is_buyer_maker": True,
        },
    ]
    create_test_csv(csv_file, mutated_rows)

    metrics_3 = pipeline.process_csv(csv_file)
    assert metrics_3["final_table_count"] == 2

    # Verify that the price in the database was updated
    with pipeline.db.get_connection() as conn:
        res = conn.execute(
            "SELECT price FROM core.fact_trades WHERE symbol = 'BTCUSDT' AND trade_id = 1;"
        ).fetchone()
        assert res is not None
        assert float(res[0]) == 61000.0
