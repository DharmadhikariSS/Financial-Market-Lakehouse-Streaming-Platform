"""Event-Time Streaming Processor with Bounded Out-of-Order Watermarking & Tumbling Windows.

Computes 1-minute tumbling window OHLCV and VWAP metrics using event-time timestamps.
Late records arriving beyond the 5-second watermark delay are dropped or routed to late-event logs.
Emitted candles are persisted to a real-time DuckDB analytical mart.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from src.common.config import get_settings
from src.common.logger import get_logger
from src.milestone_05_resilient_streaming_platform.dlq_router import DeadLetterQueueRouter
from src.milestone_05_resilient_streaming_platform.schema_registry import (
    SchemaRegistryClient,
    SchemaRegistryError,
    WireFormatError,
)

logger = get_logger("stream_processor")


@dataclass
class WindowAccumulator:
    """State accumulator for a single (symbol, window_start, window_end) candle."""

    symbol: str
    window_start_ms: int
    window_end_ms: int
    open_price: float = 0.0
    open_ts: int = 0
    high_price: float = float("-inf")
    low_price: float = float("inf")
    close_price: float = 0.0
    close_ts: int = 0
    base_volume: float = 0.0
    quote_volume: float = 0.0
    sum_price_volume: float = 0.0
    trade_count: int = 0
    taker_buy_count: int = 0
    trades: list[dict[str, Any]] = field(default_factory=list)

    def add_trade(self, trade: dict[str, Any]) -> None:
        """Accumulate a new trade record into the window state."""
        price = float(trade["price"])
        qty = float(trade["quantity"])
        ts = int(trade["trade_timestamp"])
        is_buyer_maker = bool(trade.get("is_buyer_maker", False))

        if self.trade_count == 0 or ts < self.open_ts:
            self.open_price = price
            self.open_ts = ts

        if self.trade_count == 0 or ts >= self.close_ts:
            self.close_price = price
            self.close_ts = ts

        if price > self.high_price:
            self.high_price = price
        if price < self.low_price:
            self.low_price = price

        self.base_volume += qty
        self.quote_volume += float(trade.get("quote_quantity", price * qty))
        self.sum_price_volume += price * qty
        self.trade_count += 1
        if not is_buyer_maker:
            self.taker_buy_count += 1

    def to_candle(self) -> dict[str, Any]:
        """Convert accumulator state to materialized market candle."""
        vwap = (
            round(self.sum_price_volume / self.base_volume, 4)
            if self.base_volume > 0
            else self.close_price
        )
        taker_ratio = (
            round(self.taker_buy_count * 100.0 / self.trade_count, 2)
            if self.trade_count > 0
            else 0.0
        )
        return {
            "symbol": self.symbol,
            "window_start": datetime.fromtimestamp(
                self.window_start_ms / 1000.0, tz=UTC
            ).isoformat(),
            "window_end": datetime.fromtimestamp(self.window_end_ms / 1000.0, tz=UTC).isoformat(),
            "open_price": round(self.open_price, 2),
            "high_price": round(self.high_price, 2),
            "low_price": round(self.low_price, 2),
            "close_price": round(self.close_price, 2),
            "base_volume": round(self.base_volume, 4),
            "quote_volume": round(self.quote_volume, 2),
            "vwap": vwap,
            "trade_count": self.trade_count,
            "taker_buy_ratio": taker_ratio,
        }


class EventTimeStreamProcessor:
    """Stream processor executing event-time tumbling window aggregations with watermarks."""

    def __init__(
        self,
        window_size_seconds: int = 60,
        watermark_delay_seconds: int = 5,
        registry_client: SchemaRegistryClient | None = None,
        dlq_router: DeadLetterQueueRouter | None = None,
        db_path: Path | None = None,
    ) -> None:
        settings = get_settings()
        self.window_size_ms = window_size_seconds * 1000
        self.watermark_delay_ms = watermark_delay_seconds * 1000
        self.registry = registry_client or SchemaRegistryClient()
        self.dlq = dlq_router or DeadLetterQueueRouter()

        self.db_path = db_path or (
            settings.BASE_DIR / "data" / "streaming" / "realtime_mart.duckdb"
        )
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Event-time tracking & watermarking
        self.max_event_time_ms: int = 0
        self.watermark_ms: int = 0

        # State storage: (symbol, window_start_ms) -> WindowAccumulator
        self.active_windows: dict[tuple[str, int], WindowAccumulator] = {}

        # Counters
        self.processed_count: int = 0
        self.late_rejected_count: int = 0
        self.emitted_windows_count: int = 0

        self._init_duckdb_schema()

    def _init_duckdb_schema(self) -> None:
        """Initialize the real-time analytical mart schema."""
        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS realtime_market_candles (
                symbol VARCHAR NOT NULL,
                window_start TIMESTAMP WITH TIME ZONE NOT NULL,
                window_end TIMESTAMP WITH TIME ZONE NOT NULL,
                open_price DOUBLE NOT NULL,
                high_price DOUBLE NOT NULL,
                low_price DOUBLE NOT NULL,
                close_price DOUBLE NOT NULL,
                base_volume DOUBLE NOT NULL,
                quote_volume DOUBLE NOT NULL,
                vwap DOUBLE NOT NULL,
                trade_count INTEGER NOT NULL,
                taker_buy_ratio DOUBLE NOT NULL,
                emitted_at TIMESTAMP WITH TIME ZONE NOT NULL,
                PRIMARY KEY (symbol, window_start)
            );
            """)

    def process_message(
        self, raw_payload: bytes, topic: str = "market.trades.v1"
    ) -> dict[str, Any] | None:
        """Process a single incoming raw streaming message with DLQ isolation."""
        # 1. Framing & Deserialization Stage
        try:
            schema_id, trade = self.registry.deserialize(raw_payload)
        except (WireFormatError, SchemaRegistryError, Exception) as exc:
            self.dlq.route_to_dlq(
                raw_payload=raw_payload,
                original_topic=topic,
                error_code="DESERIALIZATION_FAILURE",
                error_message=str(exc),
            )
            return None

        # 2. Domain Data Contract Verification
        price = trade.get("price", 0.0)
        quantity = trade.get("quantity", 0.0)
        trade_id = trade.get("trade_id")

        if trade_id is None or price <= 0 or quantity <= 0:
            self.dlq.route_to_dlq(
                raw_payload=raw_payload,
                original_topic=topic,
                error_code="DOMAIN_CONTRACT_VIOLATION",
                error_message=f"Invalid domain values: trade_id={trade_id}, price={price}, qty={quantity}",
                schema_id=schema_id,
            )
            return None

        # 3. Event-Time Extraction & Watermark Management
        event_ts = int(trade["trade_timestamp"])
        symbol = str(trade["symbol"])

        # Update maximum observed event time and watermark
        if event_ts > self.max_event_time_ms:
            self.max_event_time_ms = event_ts
            self.watermark_ms = self.max_event_time_ms - self.watermark_delay_ms

        # Check for unrecoverably late record (arrived after watermark passed)
        window_start_ms = (event_ts // self.window_size_ms) * self.window_size_ms
        window_end_ms = window_start_ms + self.window_size_ms

        if window_end_ms <= self.watermark_ms:
            self.late_rejected_count += 1
            logger.warning(
                f"[LATE RECORD DROPPED] Trade {symbol}#{trade_id} at {event_ts} arrived after "
                f"watermark {self.watermark_ms} (Window ended at {window_end_ms})"
            )
            return None

        # 4. Route to Window Accumulator
        window_key = (symbol, window_start_ms)
        if window_key not in self.active_windows:
            self.active_windows[window_key] = WindowAccumulator(
                symbol=symbol,
                window_start_ms=window_start_ms,
                window_end_ms=window_end_ms,
            )

        self.active_windows[window_key].add_trade(trade)
        self.processed_count += 1

        # 5. Check and emit closed windows based on current watermark
        self._emit_closed_windows()
        return trade

    def _emit_closed_windows(self, force_all: bool = False) -> list[dict[str, Any]]:
        """Emit windows that have closed based on watermark (or force on flush)."""
        closed_keys = []
        emitted_candles = []

        for window_key, accumulator in self.active_windows.items():
            if force_all or accumulator.window_end_ms <= self.watermark_ms:
                closed_keys.append(window_key)
                candle = accumulator.to_candle()
                emitted_candles.append(candle)
                self._persist_candle(candle)
                self.emitted_windows_count += 1

        for key in closed_keys:
            del self.active_windows[key]

        return emitted_candles

    def _persist_candle(self, candle: dict[str, Any]) -> None:
        """Upsert emitted candle into DuckDB real-time mart."""
        now_utc = datetime.now(UTC).isoformat()
        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO realtime_market_candles VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                );
                """,
                [
                    candle["symbol"],
                    candle["window_start"],
                    candle["window_end"],
                    candle["open_price"],
                    candle["high_price"],
                    candle["low_price"],
                    candle["close_price"],
                    candle["base_volume"],
                    candle["quote_volume"],
                    candle["vwap"],
                    candle["trade_count"],
                    candle["taker_buy_ratio"],
                    now_utc,
                ],
            )

    def flush(self) -> list[dict[str, Any]]:
        """Flush and materialize all currently open windows upon stream shutdown."""
        return self._emit_closed_windows(force_all=True)

    def get_realtime_candles(self, limit: int = 20) -> list[dict[str, Any]]:
        """Query materialized candles from the real-time mart."""
        with duckdb.connect(str(self.db_path)) as conn:
            res = conn.execute(
                """
                SELECT
                    symbol,
                    strftime(window_start, '%Y-%m-%d %H:%M:%S') AS start_time,
                    strftime(window_end, '%Y-%m-%d %H:%M:%S') AS end_time,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    base_volume,
                    quote_volume,
                    vwap,
                    trade_count,
                    taker_buy_ratio
                FROM realtime_market_candles
                ORDER BY window_start DESC, symbol ASC
                LIMIT ?;
                """,
                [limit],
            ).fetchall()

            candles = []
            for r in res:
                candles.append(
                    {
                        "symbol": r[0],
                        "start_time": r[1],
                        "end_time": r[2],
                        "open": r[3],
                        "high": r[4],
                        "low": r[5],
                        "close": r[6],
                        "volume": r[7],
                        "quote_volume": r[8],
                        "vwap": r[9],
                        "trades": r[10],
                        "taker_ratio": r[11],
                    }
                )
            return candles
