"""V2 Apache Beam Streaming Pipeline.

Implements unified event-time stream processing using Apache Beam Python SDK:
- Event-time timestamping with bounded out-of-order handling.
- Contract validation gate routing malformed trades to TaggedOutput DLQ side-collection.
- Fixed 60-second tumbling windows (FixedWindows).
- High-performance CombinePerKey accumulator for OHLCV & VWAP calculations.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window

logger = logging.getLogger("BeamPipeline")

ALLOWED_SYMBOLS = {"BTCUSDT", "ETHUSDT", "SOLUSDT"}


class ParseAndValidateTradeFn(beam.DoFn):
    """DoFn that validates trade contracts and routes failures to a TaggedOutput DLQ."""

    TAG_VALID = "valid"
    TAG_DLQ = "dlq"

    def process(self, element: dict[str, Any]) -> Any:
        try:
            # 1. Contract Validation
            symbol = str(element.get("symbol", "")).strip().upper()
            if symbol not in ALLOWED_SYMBOLS:
                raise ValueError(f"Unsupported symbol '{symbol}'")

            price = float(element.get("price", 0.0))
            if price <= 0.0:
                raise ValueError(f"Price must be positive, got {price}")

            volume = float(element.get("volume", 0.0))
            if volume <= 0.0:
                raise ValueError(f"Volume must be positive, got {volume}")

            # Parse event timestamp
            raw_ts = element.get("trade_timestamp")
            if isinstance(raw_ts, int | float):
                # epoch timestamp (seconds or ms)
                ts_sec = raw_ts / 1000.0 if raw_ts > 1e11 else float(raw_ts)
            elif isinstance(raw_ts, str):
                ts_sec = datetime.fromisoformat(raw_ts.replace("Z", "+00:00")).timestamp()
            else:
                ts_sec = datetime.now(UTC).timestamp()

            clean_trade = {
                "trade_id": str(element.get("trade_id", "")),
                "symbol": symbol,
                "price": price,
                "volume": volume,
                "trade_timestamp": ts_sec,
            }

            # Emit valid trade with event-time timestamp
            yield window.TimestampedValue(clean_trade, ts_sec)

        except Exception as exc:
            # Route contract violations to TaggedOutput DLQ
            dlq_record = {
                "raw_event": element,
                "error": str(exc),
                "error_type": exc.__class__.__name__,
                "quarantined_at": datetime.now(UTC).isoformat(),
            }
            yield beam.pvalue.TaggedOutput(self.TAG_DLQ, dlq_record)


class CandleAccumulator(beam.CombineFn):
    """CombineFn that accumulates OHLCV and calculates VWAP over tumbling windows."""

    def create_accumulator(self) -> dict[str, Any]:
        return {
            "first_ts": float("inf"),
            "last_ts": float("-inf"),
            "open": 0.0,
            "high": float("-inf"),
            "low": float("inf"),
            "close": 0.0,
            "volume": 0.0,
            "notional": 0.0,
            "trade_count": 0,
        }

    def add_input(self, acc: dict[str, Any], trade: dict[str, Any]) -> dict[str, Any]:
        ts = trade["trade_timestamp"]
        price = trade["price"]
        vol = trade["volume"]

        if ts < acc["first_ts"]:
            acc["first_ts"] = ts
            acc["open"] = price

        if ts >= acc["last_ts"]:
            acc["last_ts"] = ts
            acc["close"] = price

        acc["high"] = max(acc["high"], price)
        acc["low"] = min(acc["low"], price)
        acc["volume"] += vol
        acc["notional"] += price * vol
        acc["trade_count"] += 1
        return acc

    def merge_accumulators(self, accumulators: list[dict[str, Any]]) -> dict[str, Any]:
        merged = self.create_accumulator()
        for acc in accumulators:
            if acc["trade_count"] == 0:
                continue
            if acc["first_ts"] < merged["first_ts"]:
                merged["first_ts"] = acc["first_ts"]
                merged["open"] = acc["open"]
            if acc["last_ts"] >= merged["last_ts"]:
                merged["last_ts"] = acc["last_ts"]
                merged["close"] = acc["close"]

            merged["high"] = max(merged["high"], acc["high"])
            merged["low"] = min(merged["low"], acc["low"])
            merged["volume"] += acc["volume"]
            merged["notional"] += acc["notional"]
            merged["trade_count"] += acc["trade_count"]
        return merged

    def extract_output(self, acc: dict[str, Any]) -> dict[str, Any]:
        if acc["trade_count"] == 0:
            return {
                "open": 0.0,
                "high": 0.0,
                "low": 0.0,
                "close": 0.0,
                "volume": 0.0,
                "vwap": 0.0,
                "trade_count": 0,
            }
        vwap = acc["notional"] / acc["volume"] if acc["volume"] > 0 else acc["close"]
        return {
            "open": round(acc["open"], 4),
            "high": round(acc["high"], 4),
            "low": round(acc["low"], 4),
            "close": round(acc["close"], 4),
            "volume": round(acc["volume"], 4),
            "vwap": round(vwap, 4),
            "trade_count": acc["trade_count"],
        }


class FormatWindowCandleFn(beam.DoFn):
    """Format window boundaries alongside accumulated candle metrics."""

    def process(
        self,
        element: tuple[str, dict[str, Any]],
        win_param: Any = beam.DoFn.WindowParam,
    ) -> Any:
        symbol, candle = element
        start_iso = win_param.start.to_utc_datetime().isoformat()
        end_iso = win_param.end.to_utc_datetime().isoformat()

        yield {
            "window_start": start_iso,
            "window_end": end_iso,
            "symbol": symbol,
            "open": candle["open"],
            "high": candle["high"],
            "low": candle["low"],
            "close": candle["close"],
            "volume": candle["volume"],
            "vwap": candle["vwap"],
            "trade_count": candle["trade_count"],
        }


class AppendToJsonFileFn(beam.DoFn):
    """DoFn that writes elements as JSON lines to a destination file."""

    def __init__(self, target_file: str) -> None:
        self.target_file = target_file

    def process(self, element: dict[str, Any]) -> None:
        with open(self.target_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(element) + "\n")


def execute_beam_pipeline(
    trades: list[dict[str, Any]],
    window_seconds: int = 60,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    """Execute Beam streaming pipeline on trade events.

    Returns:
        dict containing 'candles' and 'dlq_records'.
    """
    import json
    import tempfile

    if output_dir is None:
        temp_dir = Path(tempfile.mkdtemp(prefix="beam_run_"))
    else:
        temp_dir = Path(output_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)

    candles_file = str(temp_dir / "candles.jsonl")
    dlq_file = str(temp_dir / "dlq.jsonl")

    # Clear prior output if exists
    for f_path in (candles_file, dlq_file):
        if Path(f_path).exists():
            Path(f_path).unlink()

    options = PipelineOptions(
        runner="DirectRunner",
    )

    with beam.Pipeline(options=options) as p:
        # 1. Ingest raw events
        raw_stream = p | "CreateEvents" >> beam.Create(trades)

        # 2. Parse, validate, assign event-time timestamps & separate DLQ
        validated = raw_stream | "ParseAndValidate" >> beam.ParDo(
            ParseAndValidateTradeFn()
        ).with_outputs(
            ParseAndValidateTradeFn.TAG_DLQ,
            main=ParseAndValidateTradeFn.TAG_VALID,
        )

        valid_trades = validated[ParseAndValidateTradeFn.TAG_VALID]
        dlq_records = validated[ParseAndValidateTradeFn.TAG_DLQ]

        # 3. Key by symbol
        keyed_trades = valid_trades | "KeyBySymbol" >> beam.Map(lambda t: (t["symbol"], t))

        # 4. Window into fixed tumbling windows
        windowed = keyed_trades | "FixedWindow" >> beam.WindowInto(
            window.FixedWindows(window_seconds)
        )

        # 5. Combine OHLCV & VWAP per window per symbol
        accumulated = windowed | "CombineCandles" >> beam.CombinePerKey(CandleAccumulator())

        # 6. Format with window boundaries
        formatted_candles = accumulated | "FormatCandles" >> beam.ParDo(FormatWindowCandleFn())

        # 7. Write to deterministic output files
        _ = formatted_candles | "WriteCandles" >> beam.ParDo(AppendToJsonFileFn(candles_file))
        _ = dlq_records | "WriteDLQ" >> beam.ParDo(AppendToJsonFileFn(dlq_file))

    # Read back collected results
    candles: list[dict[str, Any]] = []
    if Path(candles_file).exists():
        with open(candles_file, encoding="utf-8") as f:
            candles = [json.loads(line) for line in f if line.strip()]

    dlq_list: list[dict[str, Any]] = []
    if Path(dlq_file).exists():
        with open(dlq_file, encoding="utf-8") as f:
            dlq_list = [json.loads(line) for line in f if line.strip()]

    return {
        "status": "success",
        "valid_count": len(trades) - len(dlq_list),
        "dlq_count": len(dlq_list),
        "candles": candles,
        "dlq_records": dlq_list,
        "output_dir": str(temp_dir),
    }
