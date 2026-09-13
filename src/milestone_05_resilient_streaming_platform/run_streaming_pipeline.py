"""Master Pipeline Runner for Milestone 05: Resilient Streaming Platform.

Orchestrates the entire streaming lifecycle:
1. Confluent Schema Registry registration and BACKWARD compatibility verification.
2. Ingestion of 165+ streaming events with out-of-order timestamps and poison pills.
3. Event-time watermarking and 1-minute tumbling window OHLCV & VWAP calculations.
4. DLQ circuit breaking and webhook alert payload generation.
5. Deterministic historical backfill into isolated staging tables.
6. Observability metrics generation and comprehensive technical audit output.
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.common.logger import get_logger
from src.milestone_05_resilient_streaming_platform.backfill import BackfillEngine
from src.milestone_05_resilient_streaming_platform.dlq_router import DeadLetterQueueRouter
from src.milestone_05_resilient_streaming_platform.observability.metrics_exporter import (
    StreamingMetricsRegistry,
    start_metrics_server,
)
from src.milestone_05_resilient_streaming_platform.schema_registry import SchemaRegistryClient
from src.milestone_05_resilient_streaming_platform.stream_processor import EventTimeStreamProcessor
from src.milestone_05_resilient_streaming_platform.stream_producer import StreamProducer

logger = get_logger("streaming_runner")


def run_streaming_pipeline(
    total_events: int = 150,
    out_of_order_count: int = 10,
    poison_pill_count: int = 5,
    enable_http_metrics: bool = False,
) -> dict[str, Any]:
    """Execute complete end-to-end resilient streaming pipeline."""
    logger.info("=" * 60)
    logger.info("STARTING MILESTONE 05: RESILIENT STREAMING PLATFORM")
    logger.info("=" * 60)

    # 1. Initialize Components
    registry = SchemaRegistryClient()
    dlq = DeadLetterQueueRouter()
    metrics = StreamingMetricsRegistry.get_instance()
    producer = StreamProducer(registry_client=registry)
    processor = EventTimeStreamProcessor(
        window_size_seconds=60,
        watermark_delay_seconds=5,
        registry_client=registry,
        dlq_router=dlq,
    )

    if enable_http_metrics:
        try:
            start_metrics_server(port=9102)
        except Exception as e:
            logger.warning(f"Could not bind metrics server: {e}")

    # 2. Schema Evolution Demonstration & Proof
    schemas_dir = Path(__file__).parent / "schemas"
    v1_raw = json.loads((schemas_dir / "trade_event_v1.avsc").read_text(encoding="utf-8"))
    v2_raw = json.loads((schemas_dir / "trade_event_v2.avsc").read_text(encoding="utf-8"))

    logger.info("Verifying Schema Evolution compatibility...")
    compat, reason = SchemaRegistryClient.check_backward_compatibility(
        reader_schema=v2_raw, writer_schema=v1_raw
    )
    logger.info(f"Schema v2 -> v1 BACKWARD Compatibility: {compat} ({reason})")

    # 3. Generate Simulated Stream
    start_time_ms = int(time.time() * 1000) - (total_events * 300)
    stream_messages = producer.create_simulated_stream(
        total_events=total_events,
        out_of_order_count=out_of_order_count,
        poison_pill_count=poison_pill_count,
        start_time_ms=start_time_ms,
    )

    # 4. Stream Ingestion & Processing
    logger.info(f"Processing stream of {len(stream_messages)} events through event-time engine...")
    processed_count = 0
    for msg in stream_messages:
        payload = msg["payload"]
        topic = msg["topic"]
        metrics.inc_bytes(len(payload))

        result = processor.process_message(raw_payload=payload, topic=topic)
        if result:
            processed_count += 1
            metrics.inc_events(symbol=result["symbol"], status="success")
        else:
            metrics.inc_events(symbol="UNKNOWN", status="quarantined")

    # Flush any remaining active windows
    _ = processor.flush()
    metrics.inc_windows(processor.emitted_windows_count)

    dlq_summary = dlq.get_dlq_summary()
    candles = processor.get_realtime_candles(limit=10)

    # 5. Execute Historical Backfill Demonstration
    logger.info("Executing deterministic historical backfill across recent interval...")
    backfill_engine = BackfillEngine()
    backfill_start = datetime.fromtimestamp(start_time_ms / 1000.0, tz=UTC)
    backfill_end = datetime.now(UTC)

    backfill_result = backfill_engine.execute_backfill(
        from_ts=backfill_start,
        to_ts=backfill_end,
        target_table="historical_market_candles_backfill",
        dry_run=False,
    )

    # 6. Assemble Comprehensive Audit Report
    audit_summary = {
        "status": "SUCCESS",
        "total_stream_events": len(stream_messages),
        "successfully_processed": processed_count,
        "late_events_dropped": processor.late_rejected_count,
        "dlq_quarantined": dlq_summary["total_quarantined"],
        "materialized_candles_count": processor.emitted_windows_count,
        "backfill_candles_count": backfill_result["generated_candles"],
        "dlq_breakdown": dlq_summary["errors_by_code"],
        "metrics_bytes_processed": metrics.bytes_total,
    }

    print("\n" + "=" * 60)
    print("--- MILESTONE 05: STREAMING RESILIENCE AUDIT SUMMARY ---")
    print("=" * 60)
    for k, v in audit_summary.items():
        if k != "dlq_breakdown":
            print(f"  {k:<28} : {v}")

    print("\n--- DEAD-LETTER QUEUE (DLQ) QUARANTINE BREAKDOWN ---")
    for code, count in dlq_summary["errors_by_code"].items():
        print(f"  Incident [{code:<30}]: {count} occurrences")

    print("\n--- REAL-TIME MATERIALIZED 1-MINUTE CANDLES (DuckDB Mart) ---")
    for c in candles[:5]:
        print(
            f"  {c['symbol']} | {c['start_time']} -> {c['end_time']} | "
            f"O:${c['open']:<8} H:${c['high']:<8} L:${c['low']:<8} C:${c['close']:<8} | "
            f"VWAP:${c['vwap']:<9} Vol:${c['quote_volume']:<10} (Trades: {c['trades']})"
        )

    print("\n--- PROMETHEUS METRICS EXPOSITION SAMPLE ---")
    sample_metrics = metrics.generate_metrics_text().strip().splitlines()
    for m_line in sample_metrics[:10]:
        print(f"  {m_line}")

    return audit_summary


if __name__ == "__main__":
    run_streaming_pipeline()
