"""Unit tests for Debezium CDC parsing, Iceberg ACID mutations, Time Travel, and Quality Gates."""

from pathlib import Path

from src.milestone_04_lakehouse_cdc_contracts.cdc_consumer import (
    CdcMutationEvent,
    DebeziumEnvelopeParser,
)
from src.milestone_04_lakehouse_cdc_contracts.iceberg_writer import IcebergLakehouseTable
from src.milestone_04_lakehouse_cdc_contracts.quality_gates import QualityContractGate


def test_debezium_envelope_parser() -> None:
    # Test Create envelope
    create_msg = {
        "before": None,
        "after": {
            "trade_id": 501,
            "symbol": "BTCUSDT",
            "price": 65000.0,
            "quantity": 1.0,
            "quote_quantity": 65000.0,
            "trade_timestamp": "2026-03-01T12:00:00Z",
            "is_buyer_maker": False,
        },
        "op": "c",
        "ts_ms": 1772366400000,
        "source": {"lsn": 12345},
    }
    event = DebeziumEnvelopeParser.parse_event(create_msg)
    assert event is not None
    assert event.op_type == "insert"
    assert event.symbol == "BTCUSDT"
    assert event.trade_id == 501

    # Test Update envelope
    update_msg = {
        "before": create_msg["after"],
        "after": {**create_msg["after"], "price": 65500.0},
        "op": "u",
        "ts_ms": 1772366401000,
        "source": {"lsn": 12346},
    }
    up_event = DebeziumEnvelopeParser.parse_event(update_msg)
    assert up_event is not None
    assert up_event.op_type == "update"
    assert up_event.payload["price"] == 65500.0

    # Test Delete envelope
    delete_msg = {
        "before": update_msg["after"],
        "after": None,
        "op": "d",
        "ts_ms": 1772366402000,
        "source": {"lsn": 12347},
    }
    del_event = DebeziumEnvelopeParser.parse_event(delete_msg)
    assert del_event is not None
    assert del_event.op_type == "delete"


def test_quality_gate_quarantine(tmp_path: Path) -> None:
    quarantine_file = tmp_path / "quarantine.jsonl"
    gate = QualityContractGate(quarantine_file=quarantine_file)

    valid_event = CdcMutationEvent(
        op_type="insert",
        symbol="BTCUSDT",
        trade_id=1,
        payload={"trade_id": 1, "symbol": "BTCUSDT", "price": 65000.0, "quantity": 0.5},
        commit_ts_ms=1000,
        lsn=100,
    )

    poison_event_neg_price = CdcMutationEvent(
        op_type="insert",
        symbol="BTCUSDT",
        trade_id=2,
        payload={"trade_id": 2, "symbol": "BTCUSDT", "price": -50.0, "quantity": 0.5},
        commit_ts_ms=1001,
        lsn=101,
    )

    poison_event_null_id = CdcMutationEvent(
        op_type="insert",
        symbol="ETHUSDT",
        trade_id=0,
        payload={"trade_id": None, "symbol": "ETHUSDT", "price": 3500.0, "quantity": 1.0},
        commit_ts_ms=1002,
        lsn=102,
    )

    approved, quarantined_count = gate.filter_and_quarantine(
        [valid_event, poison_event_neg_price, poison_event_null_id]
    )

    assert len(approved) == 1
    assert approved[0].trade_id == 1
    assert quarantined_count == 2
    assert quarantine_file.exists()


def test_iceberg_acid_mutations_and_time_travel(tmp_path: Path) -> None:
    table_dir = tmp_path / "iceberg_table"
    table = IcebergLakehouseTable(table_dir=table_dir)

    # 1. Commit Initial Batch (2 records)
    event_1 = CdcMutationEvent(
        op_type="insert",
        symbol="BTCUSDT",
        trade_id=101,
        payload={"trade_id": 101, "symbol": "BTCUSDT", "price": 60000.0, "quantity": 1.0},
        commit_ts_ms=1000,
        lsn=1,
    )
    event_2 = CdcMutationEvent(
        op_type="insert",
        symbol="ETHUSDT",
        trade_id=102,
        payload={"trade_id": 102, "symbol": "ETHUSDT", "price": 3000.0, "quantity": 2.0},
        commit_ts_ms=1001,
        lsn=2,
    )

    res_1 = table.apply_cdc_mutations([event_1, event_2])
    snap_1_id = res_1["snapshot_id"]
    assert res_1["total_records"] == 2

    # 2. Update record 101 (Price revision to 62000.0)
    event_update = CdcMutationEvent(
        op_type="update",
        symbol="BTCUSDT",
        trade_id=101,
        payload={"trade_id": 101, "symbol": "BTCUSDT", "price": 62000.0, "quantity": 1.0},
        commit_ts_ms=2000,
        lsn=3,
    )
    res_2 = table.apply_cdc_mutations([event_update])
    assert res_2["total_records"] == 2

    # Verify updated price in current state
    current_data = table._read_active_records()
    assert current_data[("BTCUSDT", 101)]["price"] == 62000.0

    # 3. Delete record 102 (trade cancellation)
    event_delete = CdcMutationEvent(
        op_type="delete",
        symbol="ETHUSDT",
        trade_id=102,
        payload={"trade_id": 102, "symbol": "ETHUSDT"},
        commit_ts_ms=3000,
        lsn=4,
    )
    res_3 = table.apply_cdc_mutations([event_delete])
    assert res_3["total_records"] == 1
    assert ("ETHUSDT", 102) not in table._read_active_records()

    # 4. TIME TRAVEL PROOF: Query snapshot 1 (before update and delete)
    time_travel_records = table.time_travel_query(snap_1_id)
    assert len(time_travel_records) == 2
    # In snapshot 1, BTC was 60000.0 and ETH 102 was still alive!
    btc_past = [r for r in time_travel_records if r["trade_id"] == 101][0]
    eth_past = [r for r in time_travel_records if r["trade_id"] == 102][0]
    assert btc_past["price"] == 60000.0
    assert eth_past["price"] == 3000.0
