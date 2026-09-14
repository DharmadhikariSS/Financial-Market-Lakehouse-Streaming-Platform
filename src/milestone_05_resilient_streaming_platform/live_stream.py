"""Live Market Trade Stream Ingestion Engine.

Pulls live financial market trades directly from Binance public API,
serializes them through Confluent Avro wire formatting (v1 & v2 schemas),
and feeds them through the EventTimeStreamProcessor and DuckDB real-time mart.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import duckdb
import httpx

from src.common.config import get_settings
from src.common.logger import get_logger
from src.milestone_05_resilient_streaming_platform.dlq_router import DeadLetterQueueRouter
from src.milestone_05_resilient_streaming_platform.schema_registry import SchemaRegistryClient
from src.milestone_05_resilient_streaming_platform.stream_processor import EventTimeStreamProcessor

logger = get_logger("live_stream")

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
BINANCE_API_URL = "https://api.binance.com/api/v3/trades"


class LiveStreamIngestor:
    """Ingests live trade ticks from Binance and streams them into the Lakehouse mart."""

    def __init__(
        self,
        processor: EventTimeStreamProcessor | None = None,
        registry: SchemaRegistryClient | None = None,
        dlq: DeadLetterQueueRouter | None = None,
    ) -> None:
        self.settings = get_settings()
        self.registry = registry or SchemaRegistryClient()
        self.dlq = dlq or DeadLetterQueueRouter()
        self.processor = processor or EventTimeStreamProcessor(
            registry_client=self.registry,
            dlq_router=self.dlq,
        )
        self.db_path = self.settings.BASE_DIR / "data" / "streaming" / "realtime_mart.duckdb"
        self._init_raw_trades_table()

    def _init_raw_trades_table(self) -> None:
        """Initialize table for live streaming trade tape (Time & Sales)."""
        with duckdb.connect(str(self.db_path)) as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS realtime_raw_trades (
                trade_id BIGINT NOT NULL,
                symbol VARCHAR NOT NULL,
                price DOUBLE NOT NULL,
                quantity DOUBLE NOT NULL,
                quote_quantity DOUBLE NOT NULL,
                trade_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                is_buyer_maker BOOLEAN NOT NULL,
                trade_type VARCHAR DEFAULT 'MARKET',
                ingested_at TIMESTAMP WITH TIME ZONE NOT NULL,
                PRIMARY KEY (symbol, trade_id)
            );
            """)

    def fetch_live_trades(self, symbol: str, limit: int = 50) -> list[dict]:
        """Fetch latest public executions from Binance."""
        try:
            with httpx.Client(timeout=5.0) as client:
                resp = client.get(BINANCE_API_URL, params={"symbol": symbol, "limit": limit})
                if resp.status_code == 200:
                    return resp.json()
                logger.warning(f"Binance returned status {resp.status_code} for {symbol}")
                return []
        except Exception as exc:
            logger.warning(f"Failed to fetch live trades for {symbol}: {exc}")
            return []

    def ingest_live_batch(self, limit_per_symbol: int = 50) -> dict:
        """Fetch, serialize to Avro, and process live trades for all symbols."""
        total_fetched = 0
        total_ingested = 0
        now_utc = datetime.now(UTC).isoformat()

        trade_rows = []
        for symbol in SYMBOLS:
            raw_trades = self.fetch_live_trades(symbol=symbol, limit=limit_per_symbol)
            total_fetched += len(raw_trades)

            for item in raw_trades:
                trade_dict = {
                    "trade_id": int(item["id"]),
                    "symbol": symbol,
                    "price": float(item["price"]),
                    "quantity": float(item["qty"]),
                    "quote_quantity": float(item["quoteQty"]),
                    "trade_timestamp": int(item["time"]),
                    "is_buyer_maker": bool(item["isBuyerMaker"]),
                    "trade_type": "MARKET",
                }

                # 1. Serialize to Confluent Wire Format (Avro v2)
                payload_bytes = self.registry.serialize(
                    "market.trades-value", trade_dict, schema_id=2
                )

                # 2. Process through Stream Processor (watermark + tumbling windows)
                res = self.processor.process_message(payload_bytes, topic="market.trades.v1")
                if res:
                    total_ingested += 1

                # 3. Buffer row for batched DuckDB insert
                dt_obj = datetime.fromtimestamp(
                    float(str(trade_dict["trade_timestamp"])) / 1000.0, tz=UTC
                ).isoformat()
                trade_rows.append(
                    (
                        trade_dict["trade_id"],
                        trade_dict["symbol"],
                        trade_dict["price"],
                        trade_dict["quantity"],
                        trade_dict["quote_quantity"],
                        dt_obj,
                        trade_dict["is_buyer_maker"],
                        trade_dict["trade_type"],
                        now_utc,
                    )
                )

        # Single batched transaction to prevent Windows file locking
        if trade_rows:
            with duckdb.connect(str(self.db_path)) as conn:
                conn.executemany(
                    """
                INSERT OR REPLACE INTO realtime_raw_trades VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?
                );
                """,
                    trade_rows,
                )

        # Flush active windows to persist updated candles
        self.processor.flush()

        logger.info(f"Ingested live batch: {total_ingested}/{total_fetched} trades processed.")
        return {
            "fetched": total_fetched,
            "ingested": total_ingested,
            "timestamp": now_utc,
        }

    def seed_historical_klines(
        self, symbol: str = "BTCUSDT", interval: str = "1m", limit: int = 60
    ) -> int:
        """Fetch authentic 1-minute historical candles from Binance and persist to DuckDB mart."""
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol.upper()}&interval={interval}&limit={limit}"
        try:
            with httpx.Client(timeout=6.0) as client:
                resp = client.get(url)
                if resp.status_code != 200:
                    logger.warning(f"Binance klines API returned {resp.status_code} for {symbol}")
                    return 0
                klines = resp.json()

            rows = []
            now_utc = datetime.now(UTC).isoformat()
            for k in klines:
                open_ts = int(k[0])
                close_ts = int(k[6])
                open_p = float(k[1])
                high_p = float(k[2])
                low_p = float(k[3])
                close_p = float(k[4])
                base_vol = float(k[5])
                quote_vol = float(k[7])
                trade_cnt = int(k[8])
                taker_base_vol = float(k[9])
                taker_ratio = (taker_base_vol / base_vol * 100.0) if base_vol > 0 else 50.0
                vwap = (quote_vol / base_vol) if base_vol > 0 else close_p

                dt_start = datetime.fromtimestamp(open_ts / 1000.0, tz=UTC).isoformat()
                dt_end = datetime.fromtimestamp(close_ts / 1000.0, tz=UTC).isoformat()

                rows.append(
                    (
                        symbol.upper(),
                        dt_start,
                        dt_end,
                        open_p,
                        high_p,
                        low_p,
                        close_p,
                        round(base_vol, 4),
                        round(quote_vol, 2),
                        round(vwap, 2),
                        trade_cnt,
                        round(taker_ratio, 2),
                        now_utc,
                    )
                )

            with duckdb.connect(str(self.db_path)) as conn:
                conn.executemany(
                    """
                INSERT OR REPLACE INTO realtime_market_candles VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                );
                """,
                    rows,
                )
            logger.info(f"Successfully seeded {len(rows)} authentic klines for {symbol}.")
            return len(rows)
        except Exception as exc:
            logger.warning(f"Failed to seed klines for {symbol}: {exc}")
            return 0

    def get_order_book_depth(self, symbol: str = "BTCUSDT", limit: int = 100) -> dict[str, Any]:
        """Fetch real-time order book depth from Binance API or fallback to simulated depth."""
        url = f"https://api.binance.com/api/v3/depth?symbol={symbol.upper()}&limit={limit}"
        try:
            res = httpx.get(url, timeout=3.0)
            res.raise_for_status()
            data = res.json()
            bids = [[float(p), float(q)] for p, q in data.get("bids", [])]
            asks = [[float(p), float(q)] for p, q in data.get("asks", [])]
            return {
                "symbol": symbol.upper(),
                "bids": bids,
                "asks": asks,
                "source": "live_binance",
            }
        except Exception as exc:
            logger.warning("Falling back to synthetic order book: %s", exc)
            # Realistic synthetic fallback with smooth exponential wall
            base_price = (
                77600.0 if symbol == "BTCUSDT" else 2515.0 if symbol == "ETHUSDT" else 101.5
            )
            step = 1.0 if symbol == "BTCUSDT" else 0.1 if symbol == "ETHUSDT" else 0.02
            bids = [
                [round(base_price - i * step, 2), round(0.2 + (i**1.3) * 0.05, 4)]
                for i in range(1, limit + 1)
            ]
            asks = [
                [round(base_price + i * step, 2), round(0.2 + (i**1.3) * 0.05, 4)]
                for i in range(1, limit + 1)
            ]
            return {
                "symbol": symbol.upper(),
                "bids": bids,
                "asks": asks,
                "source": "fallback_model",
            }
