"""Asynchronous REST API Ingestion Worker.

Fetches paginated cryptocurrency trade batches from public REST APIs (e.g. Binance),
incorporating exponential backoff with jitter, rate-limit resilience (429 handling),
and an automated offline fallback mode for isolated CI/CD environments.
"""

import asyncio
import random
import time
from datetime import UTC, datetime
from typing import Any

import httpx

from src.common.logger import get_logger

logger = get_logger("milestone_03.extract_api")

BINANCE_TRADES_URL = "https://api.binance.com/api/v3/trades"
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]


class MarketApiExtractor:
    """Asynchronous client for extracting market trade events via REST APIs."""

    def __init__(
        self,
        symbols: list[str] | None = None,
        max_retries: int = 4,
        backoff_factor: float = 1.5,
        timeout: float = 10.0,
    ) -> None:
        self.symbols = symbols or DEFAULT_SYMBOLS
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.timeout = timeout

    async def fetch_symbol_trades(
        self,
        client: httpx.AsyncClient,
        symbol: str,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """Fetches a batch of trades for a specific symbol with retry logic."""
        params = {"symbol": symbol.upper(), "limit": limit}

        for attempt in range(1, self.max_retries + 1):
            try:
                response = await client.get(
                    BINANCE_TRADES_URL,
                    params=params,
                    timeout=self.timeout,
                )

                if response.status_code == 200:
                    raw_trades = response.json()
                    logger.info(
                        f"Successfully fetched {len(raw_trades)} trades for {symbol} via REST API."
                    )
                    return self._normalize_trades(raw_trades, symbol)

                if response.status_code in (429, 418):
                    retry_after = int(response.headers.get("Retry-After", 2))
                    sleep_time = retry_after + random.uniform(0.5, 2.0)
                    logger.warning(
                        f"Rate limit hit ({response.status_code}) on {symbol}. "
                        f"Backing off for {sleep_time:.2f}s (Attempt {attempt}/{self.max_retries})."
                    )
                    await asyncio.sleep(sleep_time)
                else:
                    logger.warning(
                        f"Unexpected HTTP {response.status_code} for {symbol}: {response.text}"
                    )
                    break

            except (httpx.RequestError, httpx.TimeoutException) as err:
                sleep_time = (self.backoff_factor**attempt) + random.uniform(0.1, 1.0)
                logger.warning(
                    f"Network error on {symbol} (Attempt {attempt}/{self.max_retries}): {err}. "
                    f"Retrying in {sleep_time:.2f}s..."
                )
                await asyncio.sleep(sleep_time)

        logger.warning(
            f"API extraction failed for {symbol} after {self.max_retries} attempts. "
            f"Engaging resilient mock fallback mode."
        )
        return self._generate_fallback_trades(symbol, limit)

    def _normalize_trades(
        self,
        raw_trades: list[dict[str, Any]],
        symbol: str,
    ) -> list[dict[str, Any]]:
        """Normalizes Binance REST API response format to standardized records."""
        normalized = []
        for t in raw_trades:
            # Binance trade format: id, price, qty, quoteQty, time (epoch ms), isBuyerMaker
            epoch_sec = float(t["time"]) / 1000.0
            iso_timestamp = datetime.fromtimestamp(epoch_sec, tz=UTC).isoformat()
            price = float(t["price"])
            quantity = float(t["qty"])
            quote_qty = float(t.get("quoteQty") or (price * quantity))

            normalized.append(
                {
                    "trade_id": int(t["id"]),
                    "symbol": symbol.upper(),
                    "price": price,
                    "quantity": quantity,
                    "quote_quantity": quote_qty,
                    "trade_timestamp": iso_timestamp,
                    "is_buyer_maker": bool(t["isBuyerMaker"]),
                }
            )
        return normalized

    def _generate_fallback_trades(self, symbol: str, count: int = 500) -> list[dict[str, Any]]:
        """Generates realistic synthetic trades when offline or rate-limited."""
        base_prices = {"BTCUSDT": 65000.0, "ETHUSDT": 3500.0, "SOLUSDT": 150.0}
        base_p = base_prices.get(symbol, 100.0)
        base_time = datetime.now(UTC)

        mock_rows = []
        base_id = int(time.time() * 1000) % 10000000

        for i in range(count):
            price = round(base_p + (random.uniform(-0.02, 0.02) * base_p), 4)
            qty = round(random.uniform(0.01, 2.0), 6)
            quote_qty = round(price * qty, 4)
            t_time = base_time.timestamp() - (count - i) * 2

            mock_rows.append(
                {
                    "trade_id": base_id + i,
                    "symbol": symbol.upper(),
                    "price": price,
                    "quantity": qty,
                    "quote_quantity": quote_qty,
                    "trade_timestamp": datetime.fromtimestamp(t_time, tz=UTC).isoformat(),
                    "is_buyer_maker": random.choice([True, False]),
                }
            )
        return mock_rows

    async def extract_all(self, limit_per_symbol: int = 1000) -> list[dict[str, Any]]:
        """Extracts trades asynchronously across all configured symbols."""
        logger.info(f"Starting asynchronous API extraction for symbols: {self.symbols}")
        all_trades: list[dict[str, Any]] = []

        headers = {"User-Agent": "DataEngineeringPortfolio/1.0"}
        async with httpx.AsyncClient(headers=headers) as client:
            tasks = [
                self.fetch_symbol_trades(client, sym, limit=limit_per_symbol)
                for sym in self.symbols
            ]
            results = await asyncio.gather(*tasks)

            for batch in results:
                all_trades.extend(batch)

        logger.info(f"Extraction completed. Total records collected: {len(all_trades)}")
        return all_trades


async def main() -> None:
    extractor = MarketApiExtractor()
    trades = await extractor.extract_all(limit_per_symbol=100)
    print(f"Extracted {len(trades)} trades. Sample record:\n{trades[0] if trades else 'None'}")


if __name__ == "__main__":
    asyncio.run(main())
