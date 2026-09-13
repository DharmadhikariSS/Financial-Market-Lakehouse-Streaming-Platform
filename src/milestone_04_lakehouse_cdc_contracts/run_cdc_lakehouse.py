"""Milestone 04 Orchestrator: End-to-End CDC to Iceberg Lakehouse Pipeline.

Executes:
1. Debezium CDC stream generation / reception
2. Data Quality contract evaluation & quarantine routing (Option B)
3. Apache Iceberg ACID commit & snapshot merge
4. Time Travel verification
5. Semantic layer query inspection
"""

from src.common.logger import get_logger
from src.milestone_04_lakehouse_cdc_contracts.cdc_consumer import (
    DebeziumEnvelopeParser,
    generate_mock_cdc_stream,
)
from src.milestone_04_lakehouse_cdc_contracts.iceberg_writer import IcebergLakehouseTable
from src.milestone_04_lakehouse_cdc_contracts.quality_gates import QualityContractGate
from src.milestone_04_lakehouse_cdc_contracts.semantic_layer import SemanticQueryLayer

logger = get_logger("milestone_04.orchestrator")


def run_cdc_pipeline() -> dict[str, object]:
    """Executes the complete CDC-to-Lakehouse pipeline."""
    logger.info("==================================================")
    logger.info("STARTING MILESTONE 04: CDC TO ICEBERG LAKEHOUSE")
    logger.info("==================================================")

    # 1. Generate realistic Debezium CDC Stream (100 inserts, 15 updates, 5 deletes, 5 poison pills)
    raw_stream = generate_mock_cdc_stream(
        insert_count=100,
        update_count=15,
        delete_count=5,
        poison_pill_count=5,
    )
    logger.info(f"Captured {len(raw_stream)} raw Debezium CDC change events from WAL stream.")

    # 2. Parse CDC Envelopes
    parsed_events = []
    for msg in raw_stream:
        ev = DebeziumEnvelopeParser.parse_event(msg)
        if ev:
            parsed_events.append(ev)

    logger.info(f"Parsed {len(parsed_events)} valid mutation event envelopes.")

    # 3. Apply Quality Contracts & Quarantine (Option B)
    gate = QualityContractGate()
    valid_mutations, quarantined_count = gate.filter_and_quarantine(parsed_events)

    # 4. Apply Mutations to Apache Iceberg Table (ACID Merge)
    table = IcebergLakehouseTable()
    initial_snapshot_id = table.get_current_snapshot_id()

    commit_summary = table.apply_cdc_mutations(valid_mutations)
    new_snapshot_id = commit_summary["snapshot_id"]

    # 5. Verify Time Travel
    time_travel_records = []
    if initial_snapshot_id != -1:
        time_travel_records = table.time_travel_query(initial_snapshot_id)
        logger.info(
            f"Time Travel query to Initial Snapshot {initial_snapshot_id}: "
            f"Table had {len(time_travel_records)} records before this commit."
        )

    # 6. Query Semantic Layer
    semantic = SemanticQueryLayer(table)
    daily_metrics = semantic.query_view("v_daily_market_metrics")

    results: dict[str, object] = {
        "status": "SUCCESS",
        "total_cdc_events": len(raw_stream),
        "valid_mutations": len(valid_mutations),
        "quarantined_events": quarantined_count,
        "committed_snapshot_id": new_snapshot_id,
        "active_lakehouse_rows": commit_summary["total_records"],
        "daily_metrics_rows": len(daily_metrics),
    }

    print("\n--- MILESTONE 04: CDC TO ICEBERG AUDIT SUMMARY ---")
    for k, v in results.items():
        print(f"  {k:25}: {v}")

    print("\n--- SEMANTIC VIEW: DAILY VWAP METRICS ---")
    for row in daily_metrics:
        print(
            f"  Symbol: {row['symbol']:8} | Date: {row['trade_date']} | "
            f"Trades: {row['total_trades']:4} | VWAP: ${row['vwap']:10.2f} | "
            f"Volume: ${row['quote_volume']:12,.2f} | Taker Ratio: {row['taker_buy_ratio']}%"
        )

    return results


if __name__ == "__main__":
    run_cdc_pipeline()
