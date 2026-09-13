"""Unit tests for V2 Enterprise Apache Stack.

Tests Apache Beam stream windowing & TaggedOutput DLQ,
PySpark rolling multi-asset VWAP,
PyIceberg 0.12 ACID snapshots,
and Apache Arrow Flight zero-copy gRPC RecordBatch streams.
"""

from __future__ import annotations

import tempfile
import threading
import time
from pathlib import Path

import pyarrow as pa

from src.v2_apache_ecosystem.beam_pipeline import execute_beam_pipeline
from src.v2_apache_ecosystem.flight_client import MarketFlightClient
from src.v2_apache_ecosystem.flight_server import start_flight_server
from src.v2_apache_ecosystem.pyiceberg_catalog import PyIcebergLakehouseManager
from src.v2_apache_ecosystem.spark_lakehouse import (
    TRADE_SCHEMA,
    compute_rolling_vwap,
    get_spark_session,
)


def test_beam_windowing_and_dlq() -> None:
    """Test Beam pipeline tumbling window aggregation and TaggedOutput DLQ diversion."""
    trades = [
        {
            "trade_id": "T1",
            "symbol": "BTCUSDT",
            "price": 60000.0,
            "volume": 1.0,
            "trade_timestamp": 1700000000,
        },
        {
            "trade_id": "T2",
            "symbol": "BTCUSDT",
            "price": 62000.0,
            "volume": 1.0,
            "trade_timestamp": 1700000030,
        },
        {
            "trade_id": "BAD_T3",
            "symbol": "BTCUSDT",
            "price": -500.0,  # Negative price violation
            "volume": 1.0,
            "trade_timestamp": 1700000045,
        },
    ]

    res = execute_beam_pipeline(trades, window_seconds=60)

    assert res["status"] == "success"
    assert res["valid_count"] == 2
    assert res["dlq_count"] == 1

    # Verify candle calculations
    assert len(res["candles"]) == 1
    candle = res["candles"][0]
    assert candle["symbol"] == "BTCUSDT"
    assert candle["open"] == 60000.0
    assert candle["close"] == 62000.0
    assert candle["high"] == 62000.0
    assert candle["low"] == 60000.0
    assert candle["volume"] == 2.0
    assert candle["vwap"] == 61000.0

    # Verify DLQ payload
    assert len(res["dlq_records"]) == 1
    dlq_item = res["dlq_records"][0]
    assert "Price must be positive" in dlq_item["error"]
    assert dlq_item["raw_event"]["trade_id"] == "BAD_T3"


def test_spark_rolling_vwap() -> None:
    """Test PySpark 3.5 multi-asset 1-hour rolling VWAP window function."""
    spark = get_spark_session(app_name="TestSparkRollingVWAP")
    try:
        base_ts = 1700000000
        data = [
            ("T1", "BTCUSDT", 60000.0, 1.0, base_ts),
            ("T2", "BTCUSDT", 62000.0, 1.0, base_ts + 60),
            ("T3", "ETHUSDT", 3000.0, 5.0, base_ts + 10),
        ]
        df = spark.createDataFrame(data, schema=TRADE_SCHEMA)
        enriched = compute_rolling_vwap(spark, df)
        rows = {r["trade_id"]: r for r in enriched.collect()}

        # T1 rolling vwap = 60000.0
        assert rows["T1"]["rolling_1hr_vwap"] == 60000.0
        # T2 rolling vwap = (60000*1 + 62000*1) / 2 = 61000.0
        assert rows["T2"]["rolling_1hr_vwap"] == 61000.0
        # T3 ETH rolling vwap = 3000.0
        assert rows["T3"]["rolling_1hr_vwap"] == 3000.0
    finally:
        spark.stop()


def test_pyiceberg_acid_commit_and_query() -> None:
    """Test PyIceberg official catalog table creation, append, and scan."""
    temp_wh = Path(tempfile.mkdtemp(prefix="iceberg_wh_test_"))
    mgr = PyIcebergLakehouseManager(warehouse_dir=temp_wh, catalog_name="test_cat_pytest")

    sample_trades = [
        {
            "trade_id": "ICE-01",
            "symbol": "BTCUSDT",
            "price": 64500.0,
            "volume": 0.25,
            "trade_timestamp": "2026-09-14T01:00:00Z",
            "row_hash": "hash-ice-01",
        },
        {
            "trade_id": "ICE-02",
            "symbol": "SOLUSDT",
            "price": 155.0,
            "volume": 12.0,
            "trade_timestamp": "2026-09-14T01:00:05Z",
            "row_hash": "hash-ice-02",
        },
    ]

    # ACID Commit 1
    commit1 = mgr.append_trades(sample_trades)
    assert commit1["status"] == "committed"
    assert commit1["appended_rows"] == 2
    assert commit1["snapshot_id"] is not None

    # Verify history
    history = mgr.get_snapshot_history()
    assert len(history) == 1

    # Scan and verify PyArrow Table
    scanned_table = mgr.query_table()
    assert isinstance(scanned_table, pa.Table)
    assert scanned_table.num_rows == 2


def test_arrow_flight_grpc_stream() -> None:
    """Test Arrow Flight gRPC server and client zero-copy stream."""
    test_port = 8819
    server = start_flight_server(port=test_port)
    srv_thread = threading.Thread(target=server.serve, daemon=True)
    srv_thread.start()
    time.sleep(0.3)

    try:
        client = MarketFlightClient(location=f"grpc://127.0.0.1:{test_port}")
        streams = client.list_streams()
        assert "CANDLES_1MIN_STREAM" in streams
        assert "LEDGER_TRADES_AUDIT" in streams

        candles = client.get_candles()
        assert isinstance(candles, pa.Table)
        assert candles.num_rows > 0

        trades = client.get_trades_ledger()
        assert isinstance(trades, pa.Table)
        assert trades.num_rows == 1000
    finally:
        server.shutdown()
