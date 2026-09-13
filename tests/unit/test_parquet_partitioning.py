"""Unit tests verifying Parquet lakehouse partitioning and schema conformity."""

from pathlib import Path

import duckdb

from src.milestone_03_warehouse_modular_mart.extract_api import MarketApiExtractor
from src.milestone_03_warehouse_modular_mart.parquet_writer import ParquetLakeWriter


def test_api_normalization() -> None:
    raw_api_payload = [
        {
            "id": 123456,
            "price": "65100.50",
            "qty": "0.500",
            "quoteQty": "32550.25",
            "time": 1772366400000,
            "isBuyerMaker": True,
            "isBestMatch": True,
        }
    ]
    extractor = MarketApiExtractor()
    normalized = extractor._normalize_trades(raw_api_payload, "BTCUSDT")

    assert len(normalized) == 1
    assert normalized[0]["trade_id"] == 123456
    assert normalized[0]["symbol"] == "BTCUSDT"
    assert normalized[0]["price"] == 65100.50
    assert normalized[0]["quantity"] == 0.500
    assert normalized[0]["is_buyer_maker"] is True
    assert "2026-" in normalized[0]["trade_timestamp"]


def test_parquet_hive_partition_structure(tmp_path: Path) -> None:
    writer = ParquetLakeWriter(base_output_dir=tmp_path)

    sample_trades = [
        {
            "trade_id": 101,
            "symbol": "BTCUSDT",
            "price": 65000.0,
            "quantity": 1.0,
            "quote_quantity": 65000.0,
            "trade_timestamp": "2026-03-01T12:00:00+00:00",
            "is_buyer_maker": True,
        },
        {
            "trade_id": 102,
            "symbol": "ETHUSDT",
            "price": 3500.0,
            "quantity": 2.0,
            "quote_quantity": 7000.0,
            "trade_timestamp": "2026-03-01T14:30:00+00:00",
            "is_buyer_maker": False,
        },
    ]

    written_files = writer.write_partitioned_trades(sample_trades)
    assert len(written_files) == 2

    # Check Hive directory path structure
    btc_file = [f for f in written_files if "symbol=BTCUSDT" in str(f)][0]
    eth_file = [f for f in written_files if "symbol=ETHUSDT" in str(f)][0]

    assert btc_file.exists()
    assert eth_file.exists()
    assert "year=2026" in str(btc_file)
    assert "month=03" in str(btc_file)
    assert "day=01" in str(btc_file)

    # Verify queryability with DuckDB
    conn = duckdb.connect()
    glob_path = str(tmp_path / "**/*.parquet").replace("\\", "/")
    res = conn.execute(
        f"SELECT symbol, COUNT(*), SUM(quantity) FROM read_parquet('{glob_path}', hive_partitioning=1) GROUP BY symbol ORDER BY symbol;"
    ).fetchall()

    assert len(res) == 2
    assert res[0] == ("BTCUSDT", 1, 1.0)
    assert res[1] == ("ETHUSDT", 1, 2.0)
    conn.close()
