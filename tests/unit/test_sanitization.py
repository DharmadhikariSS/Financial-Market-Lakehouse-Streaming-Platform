"""Unit tests for row sanitization, validation, and hash generation."""

from src.milestone_01_baseline_scripted_ingestion.pipeline import compute_record_hash, sanitize_row


def test_sanitize_valid_row() -> None:
    raw = {
        "trade_id": "1001",
        "symbol": "BTCUSDT",
        "price": "65123.50",
        "quantity": "0.1500",
        "quote_quantity": "9768.525",
        "trade_timestamp": "2026-03-01T12:00:00Z",
        "is_buyer_maker": "true",
    }
    result = sanitize_row(raw)
    assert result is not None
    assert result["trade_id"] == 1001
    assert result["symbol"] == "BTCUSDT"
    assert result["price"] == 65123.50
    assert result["quantity"] == 0.1500
    assert result["is_buyer_maker"] is True
    assert len(result["record_hash"]) == 64


def test_sanitize_mixed_case_and_padding_symbol() -> None:
    raw = {
        "trade_id": "1002",
        "symbol": "  btcusdt  ",
        "price": "65000",
        "quantity": "1.0",
        "trade_timestamp": "2026-03-01T12:00:00+00:00",
        "is_buyer_maker": "false",
    }
    result = sanitize_row(raw)
    assert result is not None
    assert result["symbol"] == "BTCUSDT"


def test_sanitize_scientific_notation_float() -> None:
    raw = {
        "trade_id": "1003",
        "symbol": "ETHUSDT",
        "price": "3.55e3",  # 3550.0
        "quantity": "1.25e-1",  # 0.125
        "trade_timestamp": "2026-03-01T12:00:00Z",
        "is_buyer_maker": "0",
    }
    result = sanitize_row(raw)
    assert result is not None
    assert result["price"] == 3550.0
    assert result["quantity"] == 0.125
    assert result["is_buyer_maker"] is False


def test_sanitize_epoch_millisecond_timestamp() -> None:
    raw = {
        "trade_id": "1004",
        "symbol": "SOLUSDT",
        "price": "150.25",
        "quantity": "2.0",
        "trade_timestamp": "1772452800000",  # epoch ms
        "is_buyer_maker": "t",
    }
    result = sanitize_row(raw)
    assert result is not None
    assert "2026-" in result["trade_timestamp"]


def test_reject_corrupt_trade_id() -> None:
    raw = {
        "trade_id": "-5",  # Invalid ID
        "symbol": "BTCUSDT",
        "price": "65000",
        "quantity": "1.0",
        "trade_timestamp": "2026-03-01T12:00:00Z",
    }
    assert sanitize_row(raw) is None


def test_reject_negative_price() -> None:
    raw = {
        "trade_id": "1005",
        "symbol": "BTCUSDT",
        "price": "-100.0",  # Invalid negative price
        "quantity": "1.0",
        "trade_timestamp": "2026-03-01T12:00:00Z",
    }
    assert sanitize_row(raw) is None


def test_record_hash_deterministic() -> None:
    hash_1 = compute_record_hash("BTCUSDT", 1001, 65000.0, 1.0, "2026-03-01T12:00:00+00:00", True)
    hash_2 = compute_record_hash("BTCUSDT", 1001, 65000.0, 1.0, "2026-03-01T12:00:00+00:00", True)
    hash_diff = compute_record_hash(
        "BTCUSDT", 1001, 65001.0, 1.0, "2026-03-01T12:00:00+00:00", True
    )

    assert hash_1 == hash_2
    assert hash_1 != hash_diff
