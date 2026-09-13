"""Unit tests for Milestone 05: Resilient Streaming Platform.

Tests Avro Confluent wire format serialization, schema evolution (v1 -> v2),
event-time watermarking with tumbling windows, Dead-Letter Queue (DLQ) quarantine,
and deterministic historical backfill.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from src.milestone_05_resilient_streaming_platform.backfill import BackfillEngine
from src.milestone_05_resilient_streaming_platform.dlq_router import DeadLetterQueueRouter
from src.milestone_05_resilient_streaming_platform.schema_registry import (
    IncompatibleSchemaError,
    SchemaRegistryClient,
)
from src.milestone_05_resilient_streaming_platform.stream_processor import EventTimeStreamProcessor


@pytest.fixture
def temp_streaming_env(tmp_path: Path):
    schemas_dir = (
        Path(__file__).parent.parent.parent
        / "src"
        / "milestone_05_resilient_streaming_platform"
        / "schemas"
    )
    registry = SchemaRegistryClient(schemas_dir=schemas_dir)
    dlq_dir = tmp_path / "dlq"
    dlq = DeadLetterQueueRouter(dlq_dir=dlq_dir)
    db_path = tmp_path / "test_realtime.duckdb"
    lakehouse_dir = tmp_path / "lakehouse"
    lakehouse_dir.mkdir(parents=True, exist_ok=True)

    processor = EventTimeStreamProcessor(
        window_size_seconds=60,
        watermark_delay_seconds=5,
        registry_client=registry,
        dlq_router=dlq,
        db_path=db_path,
    )

    return {
        "registry": registry,
        "dlq": dlq,
        "processor": processor,
        "db_path": db_path,
        "lakehouse_dir": lakehouse_dir,
    }


def test_schema_registry_confluent_wire_format(temp_streaming_env):
    """Verify Confluent wire framing (0x00 + 4-byte Schema ID + Avro payload)."""
    registry: SchemaRegistryClient = temp_streaming_env["registry"]
    trade = {
        "trade_id": 10001,
        "symbol": "BTCUSDT",
        "price": 65000.0,
        "quantity": 1.5,
        "quote_quantity": 97500.0,
        "trade_timestamp": 1789300000000,
        "is_buyer_maker": False,
    }

    # Serialize with schema v1 (id = 1)
    payload = registry.serialize("market.trades-value", trade, schema_id=1)
    assert len(payload) > 5
    assert payload[0:1] == b"\x00"  # Magic byte

    # Deserialize and verify fields
    schema_id, decoded = registry.deserialize(payload)
    assert schema_id == 1
    assert decoded["trade_id"] == 10001
    assert decoded["symbol"] == "BTCUSDT"
    assert decoded["price"] == 65000.0
    assert decoded["quantity"] == 1.5


def test_schema_evolution_backward_compatibility(temp_streaming_env):
    """Verify v2 reader can deserialize v1 data seamlessly with default values."""
    registry: SchemaRegistryClient = temp_streaming_env["registry"]

    # Trade written with Schema v1 (does NOT have trade_type field)
    v1_trade = {
        "trade_id": 10002,
        "symbol": "ETHUSDT",
        "price": 3500.0,
        "quantity": 2.0,
        "quote_quantity": 7000.0,
        "trade_timestamp": 1789300005000,
        "is_buyer_maker": True,
    }
    payload_v1 = registry.serialize("market.trades-value", v1_trade, schema_id=1)

    # Read using Schema v2 (has optional trade_type with default "MARKET")
    schema_id, decoded = registry.deserialize(payload_v1, reader_schema_id=2)
    assert schema_id == 1
    assert decoded["symbol"] == "ETHUSDT"
    assert decoded["trade_type"] == "MARKET"  # Populated by default in v2 reader

    # Incompatible schema rejection test: mandatory field without default
    incompatible_schema = {
        "type": "record",
        "name": "TradeEvent",
        "namespace": "io.market.ledger",
        "fields": [
            {"name": "trade_id", "type": "long"},
            {"name": "mandatory_missing_field", "type": "string"},  # No default!
        ],
    }
    with pytest.raises(IncompatibleSchemaError):
        registry.register("market.trades-value", incompatible_schema)


def test_event_time_watermark_and_tumbling_window(temp_streaming_env):
    """Verify 1-minute tumbling window OHLCV & VWAP calculations and watermark rejection."""
    processor: EventTimeStreamProcessor = temp_streaming_env["processor"]
    registry: SchemaRegistryClient = temp_streaming_env["registry"]

    base_time_ms = 1789300000000  # Minute boundary: 1789300000000 // 60000 * 60000

    # 1. Feed 3 sequential trades in the same minute window
    t1 = {
        "trade_id": 1,
        "symbol": "BTCUSDT",
        "price": 60000.0,
        "quantity": 1.0,
        "quote_quantity": 60000.0,
        "trade_timestamp": base_time_ms + 1000,
        "is_buyer_maker": False,
        "trade_type": "MARKET",
    }
    t2 = {
        "trade_id": 2,
        "symbol": "BTCUSDT",
        "price": 62000.0,
        "quantity": 2.0,
        "quote_quantity": 124000.0,
        "trade_timestamp": base_time_ms + 5000,
        "is_buyer_maker": True,
        "trade_type": "MARKET",
    }
    t3 = {
        "trade_id": 3,
        "symbol": "BTCUSDT",
        "price": 59000.0,
        "quantity": 1.0,
        "quote_quantity": 59000.0,
        "trade_timestamp": base_time_ms + 15000,
        "is_buyer_maker": False,
        "trade_type": "MARKET",
    }

    p1 = registry.serialize("market.trades-value", t1, schema_id=2)
    p2 = registry.serialize("market.trades-value", t2, schema_id=2)
    p3 = registry.serialize("market.trades-value", t3, schema_id=2)

    assert processor.process_message(p1) is not None
    assert processor.process_message(p2) is not None
    assert processor.process_message(p3) is not None

    # 2. Advance watermark past this window (+66 seconds, past 60s window + 5s watermark delay)
    t_next_minute = {
        "trade_id": 4,
        "symbol": "BTCUSDT",
        "price": 61000.0,
        "quantity": 0.5,
        "quote_quantity": 30500.0,
        "trade_timestamp": base_time_ms + 66000,
        "is_buyer_maker": False,
        "trade_type": "MARKET",
    }
    p_next = registry.serialize("market.trades-value", t_next_minute, schema_id=2)
    processor.process_message(p_next)

    # Verify first window was closed and materialized
    candles = processor.get_realtime_candles()
    assert len(candles) >= 1
    candle = candles[-1]
    assert candle["symbol"] == "BTCUSDT"
    assert candle["open"] == 60000.0
    assert candle["high"] == 62000.0
    assert candle["low"] == 59000.0
    assert candle["close"] == 59000.0
    assert candle["volume"] == 4.0
    # VWAP = (60000*1 + 62000*2 + 59000*1) / 4 = 243000 / 4 = 60750.0
    assert candle["vwap"] == 60750.0
    assert candle["trades"] == 3

    # 3. Feed an excessively late record (arriving after watermark closed window)
    t_late = {
        "trade_id": 5,
        "symbol": "BTCUSDT",
        "price": 60500.0,
        "quantity": 1.0,
        "quote_quantity": 60500.0,
        "trade_timestamp": base_time_ms + 2000,  # Belongs to first window!
        "is_buyer_maker": False,
        "trade_type": "MARKET",
    }
    p_late = registry.serialize("market.trades-value", t_late, schema_id=2)
    res_late = processor.process_message(p_late)
    assert res_late is None
    assert processor.late_rejected_count == 1


def test_dead_letter_queue_quarantine(temp_streaming_env):
    """Verify poison pills trigger DLQ quarantine without halting the processor."""
    processor: EventTimeStreamProcessor = temp_streaming_env["processor"]
    dlq: DeadLetterQueueRouter = temp_streaming_env["dlq"]
    registry: SchemaRegistryClient = temp_streaming_env["registry"]

    # 1. Invalid Magic Byte
    bad_magic = b"\xEE\x00\x00\x00\x01\x11\x22"
    assert processor.process_message(bad_magic) is None

    # 2. Domain Contract Violation (Negative Price)
    invalid_trade = {
        "trade_id": 9999,
        "symbol": "SOLUSDT",
        "price": -100.0,
        "quantity": 2.0,
        "quote_quantity": -200.0,
        "trade_timestamp": 1789300000000,
        "is_buyer_maker": False,
    }
    bad_contract_payload = registry.serialize("market.trades-value", invalid_trade, schema_id=1)
    assert processor.process_message(bad_contract_payload) is None

    # Check DLQ storage
    records = dlq.read_dlq_records()
    assert len(records) == 2
    error_codes = [r["error_code"] for r in records]
    assert "DESERIALIZATION_FAILURE" in error_codes
    assert "DOMAIN_CONTRACT_VIOLATION" in error_codes

    # Verify diagnostic headers and base64 payload
    assert "raw_payload_b64" in records[0]
    assert records[0]["failed_at_utc"] is not None


def test_deterministic_backfill(temp_streaming_env):
    """Verify historical backfill creates isolated candle tables and honors dry-run."""
    db_path = temp_streaming_env["db_path"]
    engine = BackfillEngine(db_path=db_path)

    start_dt = datetime(2026, 9, 13, 0, 0, 0, tzinfo=UTC)
    end_dt = datetime(2026, 9, 13, 0, 10, 0, tzinfo=UTC)

    # 1. Dry run
    dry_summary = engine.execute_backfill(from_ts=start_dt, to_ts=end_dt, dry_run=True)
    assert dry_summary["status"] == "DRY_RUN_SUCCESS"
    assert dry_summary["generated_candles"] > 0

    # 2. Real commit
    commit_summary = engine.execute_backfill(
        from_ts=start_dt,
        to_ts=end_dt,
        target_table="test_historical_candles",
        dry_run=False,
    )
    assert commit_summary["status"] == "COMMITTED"
    assert commit_summary["generated_candles"] > 0
