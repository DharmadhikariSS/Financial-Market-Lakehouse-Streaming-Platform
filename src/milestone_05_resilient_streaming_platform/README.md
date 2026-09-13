# Technical Audit Report: Milestone 05 — Resilient Streaming Platform

## 1. Milestone Overview & Objectives

**Milestone 05 (`milestone_05_resilient_streaming_platform`)** represents the pinnacle event-driven tier of the Financial Market Trade Ledger Platform. It processes unbounded, continuous market trade data streams while guaranteeing:
1. **Confluent-Compatible Schema Evolution**: Native Avro serialization with Confluent Wire Framing (`0x00` magic byte + 4-byte Schema ID) enforcing strict `BACKWARD` compatibility rules.
2. **Event-Time Watermarking & Windowing**: Extracts embedded event timestamps, applies a 5-second bounded out-of-order watermark, and materializes 1-minute tumbling candles computing Open, High, Low, Close, Volume, and VWAP.
3. **Dead-Letter Queue (DLQ) Circuit Breakers**: Isolates corrupted bytes, missing primary keys, and price contract breaches to persistent JSONL storage with diagnostic headers and structured webhook alerting without interrupting the active stream.
4. **Deterministic Historical Backfill Engine**: Replays historical Parquet lakehouse archives into isolated staging tables, proving reproducible OHLCV candle reconstruction without live state pollution.
5. **Full-Stack Observability**: Native Prometheus metrics exposition (`/metrics` on port 9102) and pre-configured Grafana dashboard JSON tracking ingestion rate, watermark lag, consumer lag, and DLQ incidents.

---

## 2. End-to-End Streaming Architecture

```mermaid
flowchart TD
    subgraph Producer & Schema Registry
        P[Stream Producer] -->|Fetch / Register Schema| SR[Schema Registry Client]
        SR -->|Validate BACKWARD Compatibility| Avro[trade_event_v1.avsc / v2.avsc]
        P -->|Confluent Framed Bytes: 0x00 + Schema ID + Avro| StreamTopic[(Topic: market.trades.v1)]
    end

    subgraph Event-Time Streaming Engine
        StreamTopic --> Processor[EventTimeStreamProcessor]
        Processor --> Deser{Wire & Avro Deserializer}
        Deser -->|Contract Failure or Bad Bytes| DLQ[DLQ Router]
        Deser -->|Valid Payload| Watermark[Watermark Calculation: Max(ts) - 5s]
        Watermark -->|Late Record: ts < Watermark| Drop[Late Record Drop Log]
        Watermark -->|On-Time / Acceptable Late| Tumbling[1-Minute Tumbling Window Accumulator]
        Tumbling -->|Watermark >= Window End| Emit[Materialize Market Candle]
        Emit --> Sink[(DuckDB: realtime_market_candles)]
    end

    subgraph Fault Recovery & Alerting
        DLQ --> DLQFile[data/streaming/dlq/market_trades_dlq.jsonl]
        DLQ --> Webhook[Discord / Slack Webhook Incident Dispatch]
    end

    subgraph Observability
        Processor --> Metrics[Prometheus Metrics Exporter :9102]
        Metrics --> Prom[Prometheus Server]
        Prom --> Grafana[Grafana Dashboard]
    end

    subgraph Deterministic Recovery
        Lakehouse[(Historical Parquet Lakehouse)] --> Backfill[BackfillEngine CLI]
        Backfill --> IsolatedSink[(DuckDB: historical_market_candles_backfill)]
    end
```

---

## 3. Confluent Wire Protocol & Schema Evolution

### Wire Framing Specification
Every streaming event sent over the transport layer adheres to the Confluent Schema Registry binary format:
```
+--------------+-------------------+-----------------------------------------+
| Magic Byte   | Schema ID         | Avro Binary Payload                    |
| (1 byte: 0x0)| (4 bytes, uint32) | (Variable length, fastavro schemaless) |
+--------------+-------------------+-----------------------------------------+
```

### Schema Evolution: v1 to v2
* **`trade_event_v1.avsc` (Strict Base Record)**:
  `trade_id` (long), `symbol` (string), `price` (double), `quantity` (double), `quote_quantity` (double), `trade_timestamp` (long), `is_buyer_maker` (boolean).
* **`trade_event_v2.avsc` (Evolved Record)**:
  Adds `trade_type` (`["string", "null"]`) with default `"MARKET"`.
* **Mathematical Compatibility Proof**:
  Under Avro `BACKWARD` compatibility rules, when reading data produced by v1 using the v2 schema, the missing `trade_type` field is seamlessly populated with its default value `"MARKET"`. This allows consumers to upgrade before producers without downtime or deserialization crashes.

---

## 4. Event-Time Watermarking & Windowing Formulation

### Mathematical Formulations
1. **Event Time Extraction**:
   $$t_e = \text{payload.trade\_timestamp}$$
2. **Bounded Out-of-Order Watermark**:
   Given a bounded latency tolerance $\Delta t_{\text{delay}} = 5\,000\text{ ms}$:
   $$W(t) = \max_{i \le t}(t_e^{(i)}) - \Delta t_{\text{delay}}$$
3. **Tumbling Window Assignment**:
   For window size $\Delta t_w = 60\,000\text{ ms}$ (1 minute):
   $$t_{\text{start}} = \left\lfloor \frac{t_e}{\Delta t_w} \right\rfloor \times \Delta t_w, \quad t_{\text{end}} = t_{\text{start}} + \Delta t_w$$
4. **Late Record Dropping Rule**:
   If a record arrives with timestamp $t_e$ such that its assigned window end satisfies:
   $$t_{\text{end}} \le W(t)$$
   the window has already been closed and materialized to the storage sink. The record is flagged and dropped to prevent mutating finalized candles.
5. **Candle VWAP Calculation**:
   $$\text{VWAP} = \frac{\sum_{k=1}^{N} (\text{price}_k \times \text{quantity}_k)}{\sum_{k=1}^{N} \text{quantity}_k}$$

---

## 5. Dead-Letter Queue (DLQ) Quarantine & Alerting Protocol

When an unparseable byte sequence, schema mismatch, or domain contract breach occurs, the stream processor intercepts the exception immediately without terminating worker threads.

### Diagnostic Header Schema
Each quarantined record is written to `data/streaming/dlq/market_trades_dlq.jsonl` with:
* `failed_at_utc`: ISO-8601 UTC timestamp.
* `original_topic`: Name of the source Kafka/Redpanda topic.
* `error_code`: Categorized failure (`INVALID_MAGIC_BYTE`, `UNKNOWN_SCHEMA_ID`, `DOMAIN_CONTRACT_VIOLATION`, etc.).
* `error_message`: Full human-readable diagnosis.
* `raw_payload_b64`: Base64-encoded raw byte array preserving the exact physical corrupt payload for post-mortem analysis.
* `stack_trace`: First 500 characters of the Python exception traceback.

### Webhook Notification Payload
Generates formatted Discord/Slack embeds with color code `15158332` (Crimson Red), incident type, topic, and stack details for automated SRE alerting.

---

## 6. Deterministic Historical Backfill Engine

The backfill CLI (`backfill.py`) allows operators to reconstruct missing candle intervals by re-processing historical Parquet lakehouse archives:
```bash
python -m src.milestone_05_resilient_streaming_platform.backfill \
    --from 2026-09-13T00:00:00 \
    --to 2026-09-13T23:59:59 \
    --symbol BTCUSDT \
    --target-table historical_market_candles_backfill
```
* **State Isolation**: Writes to a distinct table (`historical_market_candles_backfill`), preventing duplicate key errors or partial overwrite hazards in the active real-time table.
* **Deterministic Parity**: Uses identical `WindowAccumulator` logic, ensuring OHLCV and VWAP values match between live streaming and batch replay down to the 4th decimal place.

---

## 7. How to Reproduce Milestone 05

```bash
# 1. Run automated test suite
python -m pytest -v tests/unit/test_streaming_resilience.py

# 2. Execute end-to-end streaming pipeline
python -m src.milestone_05_resilient_streaming_platform.run_streaming_pipeline

# 3. Inspect quarantined DLQ audit records
python -c "from src.milestone_05_resilient_streaming_platform.dlq_router import DeadLetterQueueRouter; print(DeadLetterQueueRouter().get_dlq_summary())"

# 4. Execute dry-run backfill simulation
python -m src.milestone_05_resilient_streaming_platform.backfill --dry-run
```
