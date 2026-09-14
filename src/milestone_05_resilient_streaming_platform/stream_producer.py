"""Market Trade Event Stream Producer.

Simulates live financial market trade streaming with:
- Schema Registry serialization (v1 and v2 schemas)
- Out-of-order arrival timestamps
- Controlled poison pills and malformed bytes for DLQ testing
"""

from __future__ import annotations

import random
import time
from typing import Any

from src.common.logger import get_logger
from src.milestone_05_resilient_streaming_platform.schema_registry import SchemaRegistryClient

logger = get_logger("stream_producer")

BASE_PRICES = {
    "BTCUSDT": 65000.0,
    "ETHUSDT": 3500.0,
    "SOLUSDT": 150.0,
}


class StreamProducer:
    """Produces framed Avro messages with edge cases for resilience testing."""

    def __init__(self, registry_client: SchemaRegistryClient | None = None) -> None:
        self.registry = registry_client or SchemaRegistryClient()
        self.subject = "market.trades-value"
        self._trade_seq = 500000

    def generate_trade(
        self,
        symbol: str,
        timestamp_ms: int,
        schema_version: int = 1,
        trade_type: str = "MARKET",
        price_override: float | None = None,
    ) -> dict[str, Any]:
        """Generate a synthetic financial trade dictionary."""
        self._trade_seq += 1
        base_price = BASE_PRICES.get(symbol, 100.0)
        # Add random walk +/- 0.5%
        price = (
            price_override
            if price_override is not None
            else round(base_price * (1.0 + random.uniform(-0.005, 0.005)), 2)
        )
        quantity = round(random.uniform(0.01, 2.5), 4)
        quote_quantity = round(price * quantity, 2)

        trade = {
            "trade_id": self._trade_seq,
            "symbol": symbol,
            "price": price,
            "quantity": quantity,
            "quote_quantity": quote_quantity,
            "trade_timestamp": timestamp_ms,
            "is_buyer_maker": random.choice([True, False]),
        }

        if schema_version >= 2:
            trade["trade_type"] = trade_type

        return trade

    def create_simulated_stream(
        self,
        total_events: int = 150,
        out_of_order_count: int = 10,
        poison_pill_count: int = 5,
        start_time_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        """Generate a mixed stream of valid, out-of-order, and poison-pill messages.

        Returns a list of dicts: {"topic": str, "payload": bytes, "meta": dict}
        """
        current_time_ms = start_time_ms or int(time.time() * 1000)
        messages: list[dict[str, Any]] = []

        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

        # 1. Generate regular sequential trade events
        for i in range(total_events):
            symbol = symbols[i % len(symbols)]
            # Advance event time by ~300ms per trade
            event_ts = current_time_ms + (i * 300)
            schema_version = 2 if i % 2 == 0 else 1
            trade_type = "LIMIT" if (i % 4 == 0 and schema_version == 2) else "MARKET"

            trade = self.generate_trade(
                symbol=symbol,
                timestamp_ms=event_ts,
                schema_version=schema_version,
                trade_type=trade_type,
            )

            schema_id = 2 if schema_version == 2 else 1
            payload_bytes = self.registry.serialize(self.subject, trade, schema_id=schema_id)

            messages.append(
                {
                    "topic": "market.trades.v1",
                    "payload": payload_bytes,
                    "meta": {
                        "type": "valid",
                        "trade_id": trade["trade_id"],
                        "symbol": symbol,
                        "event_ts": event_ts,
                        "schema_version": schema_version,
                    },
                }
            )

        # 2. Inject Out-of-Order Events (e.g. network latency delay)
        # Some are within 5s (late but accepted by watermark), some are >15s (rejected by watermark)
        for j in range(out_of_order_count):
            symbol = symbols[j % len(symbols)]
            # j % 2 == 0: 3-second delay (within 5s buffer)
            # j % 2 != 0: 25-second delay (outside 5s buffer, past watermark)
            delay_ms = 3000 if j % 2 == 0 else 25000
            event_ts = current_time_ms + (total_events * 300) - delay_ms

            trade = self.generate_trade(
                symbol=symbol,
                timestamp_ms=event_ts,
                schema_version=2,
                trade_type="MARKET",
            )
            payload_bytes = self.registry.serialize(self.subject, trade, schema_id=2)

            # Insert into the stream near the end
            messages.append(
                {
                    "topic": "market.trades.v1",
                    "payload": payload_bytes,
                    "meta": {
                        "type": "out_of_order",
                        "trade_id": trade["trade_id"],
                        "symbol": symbol,
                        "event_ts": event_ts,
                        "delay_ms": delay_ms,
                    },
                }
            )

        # 3. Inject Poison Pills (Corrupted payloads for DLQ)
        for k in range(poison_pill_count):
            if k == 0:
                # Malformed Magic Byte
                bad_bytes = b"\xff\x00\x00\x00\x01\x10\x20\x30\x40"
                error_desc = "INVALID_MAGIC_BYTE"
            elif k == 1:
                # Non-existent schema ID 9999
                import struct

                bad_bytes = b"\x00" + struct.pack(">I", 9999) + b"\x01\x02\x03\x04"
                error_desc = "UNKNOWN_SCHEMA_ID"
            elif k == 2:
                # Truncated Avro framing
                bad_bytes = b"\x00\x00\x01"
                error_desc = "TRUNCATED_WIRE_PAYLOAD"
            elif k == 3:
                # Negative price contract violation
                trade = self.generate_trade(
                    symbol="BTCUSDT",
                    timestamp_ms=current_time_ms + 1000,
                    schema_version=1,
                    price_override=-12500.0,
                )
                bad_bytes = self.registry.serialize(self.subject, trade, schema_id=1)
                error_desc = "DOMAIN_CONTRACT_NEGATIVE_PRICE"
            else:
                # Corrupted binary junk in Avro body
                import struct

                bad_bytes = b"\x00" + struct.pack(">I", 1) + b"\xde\xad\xbe\xef\x99\x88"
                error_desc = "AVRO_DESERIALIZATION_CORRUPTION"

            messages.append(
                {
                    "topic": "market.trades.v1",
                    "payload": bad_bytes,
                    "meta": {
                        "type": "poison_pill",
                        "error_desc": error_desc,
                    },
                }
            )

        logger.info(
            f"Generated stream batch: {len(messages)} events "
            f"({total_events} sequential, {out_of_order_count} out-of-order, {poison_pill_count} poison pills)"
        )
        return messages
