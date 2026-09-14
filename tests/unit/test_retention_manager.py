"""Unit tests for RetentionPolicyManager."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from src.maintenance.retention_manager import RetentionPolicyManager


@pytest.fixture
def mock_retention_env(tmp_path: Path):
    """Fixture providing isolated temporary DuckDB and DLQ paths."""
    db_path = tmp_path / "test_mart.duckdb"
    dlq_file = tmp_path / "market_trades_dlq.jsonl"

    # Setup DuckDB tables
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("""
            CREATE TABLE realtime_raw_trades (
                trade_id BIGINT,
                symbol VARCHAR,
                price DOUBLE,
                quantity DOUBLE,
                quote_quantity DOUBLE,
                trade_timestamp TIMESTAMP WITH TIME ZONE,
                is_buyer_maker BOOLEAN,
                trade_type VARCHAR,
                ingested_at TIMESTAMP WITH TIME ZONE
            );
            CREATE TABLE realtime_market_candles (
                symbol VARCHAR,
                window_start TIMESTAMP WITH TIME ZONE,
                window_end TIMESTAMP WITH TIME ZONE,
                open_price DOUBLE,
                high_price DOUBLE,
                low_price DOUBLE,
                close_price DOUBLE,
                base_volume DOUBLE,
                quote_volume DOUBLE,
                vwap DOUBLE,
                trade_count INTEGER,
                taker_buy_ratio DOUBLE,
                emitted_at TIMESTAMP WITH TIME ZONE
            );
        """)

        # Insert 1 recent trade and 1 old trade (48h ago)
        now_utc = datetime.now(UTC)
        old_trade_ts = (now_utc - timedelta(hours=48)).isoformat()
        recent_trade_ts = now_utc.isoformat()

        conn.execute("""
            INSERT INTO realtime_raw_trades VALUES
            (1, 'BTCUSDT', 77000.0, 1.0, 77000.0, ?, false, 'MARKET', ?),
            (2, 'BTCUSDT', 77100.0, 1.0, 77100.0, ?, false, 'MARKET', ?)
        """, [old_trade_ts, old_trade_ts, recent_trade_ts, recent_trade_ts])

    # Setup DLQ with 1 old record (40 days ago) and 1 recent record (2 days ago)
    old_dlq_dt = (now_utc - timedelta(days=40)).isoformat()
    recent_dlq_dt = (now_utc - timedelta(days=2)).isoformat()

    with open(dlq_file, "w", encoding="utf-8") as f:
        f.write(json.dumps({"failed_at_utc": old_dlq_dt, "error_code": "OLD_ERROR"}) + "\n")
        f.write(json.dumps({"failed_at_utc": recent_dlq_dt, "error_code": "RECENT_ERROR"}) + "\n")

    return {"db_path": db_path, "dlq_file": dlq_file, "tmp_path": tmp_path}


def test_prune_streaming_duckdb(mock_retention_env):
    mgr = RetentionPolicyManager(db_path=mock_retention_env["db_path"])
    res = mgr.prune_streaming_duckdb(max_trade_age_hours=24)

    assert res["deleted_trades"] == 1

    # Verify only recent trade remains
    with duckdb.connect(str(mock_retention_env["db_path"])) as conn:
        count = conn.execute("SELECT count(*) FROM realtime_raw_trades").fetchone()[0]
        assert count == 1
