"""Debezium Change Data Capture (CDC) Event Parser & Stream Generator.

Parses row-level transactional database mutations from PostgreSQL WAL events
(Create, Read, Update, Delete) adhering to the standard Debezium envelope schema.
"""

import random
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from src.common.logger import get_logger

logger = get_logger("milestone_04.cdc_consumer")


@dataclass
class CdcMutationEvent:
    """Normalized CDC Mutation representation."""

    op_type: str  # 'insert', 'update', 'delete'
    symbol: str
    trade_id: int
    payload: dict[str, Any]
    commit_ts_ms: int
    lsn: int


class DebeziumEnvelopeParser:
    """Parses raw JSON Debezium CDC messages into normalized mutation events."""

    @staticmethod
    def parse_event(raw_message: dict[str, Any]) -> CdcMutationEvent | None:
        """Parses a single Debezium event envelope."""
        try:
            op = raw_message.get("op")
            ts_ms = raw_message.get("ts_ms", int(time.time() * 1000))
            source = raw_message.get("source", {})
            lsn = source.get("lsn", int(time.time() * 1000))

            if op in ("c", "r"):
                # Create or Read (Initial snapshot)
                after = raw_message.get("after")
                if not after:
                    return None
                return CdcMutationEvent(
                    op_type="insert",
                    symbol=after["symbol"],
                    trade_id=int(after["trade_id"]),
                    payload=after,
                    commit_ts_ms=ts_ms,
                    lsn=lsn,
                )

            elif op == "u":
                # Update (price revision, status update)
                after = raw_message.get("after")
                if not after:
                    return None
                return CdcMutationEvent(
                    op_type="update",
                    symbol=after["symbol"],
                    trade_id=int(after["trade_id"]),
                    payload=after,
                    commit_ts_ms=ts_ms,
                    lsn=lsn,
                )

            elif op == "d":
                # Delete (trade cancellation/reversal)
                before = raw_message.get("before")
                if not before:
                    return None
                return CdcMutationEvent(
                    op_type="delete",
                    symbol=before["symbol"],
                    trade_id=int(before["trade_id"]),
                    payload=before,
                    commit_ts_ms=ts_ms,
                    lsn=lsn,
                )

            return None
        except (KeyError, ValueError, TypeError) as err:
            logger.warning(f"Malformed CDC envelope encountered: {err}")
            return None


def generate_mock_cdc_stream(
    insert_count: int = 100,
    update_count: int = 15,
    delete_count: int = 5,
    poison_pill_count: int = 5,
) -> list[dict[str, Any]]:
    """Generates a realistic stream of Debezium CDC events (inserts, updates, deletes, and bad data)."""
    random.seed(42)
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    base_prices = {"BTCUSDT": 65000.0, "ETHUSDT": 3500.0, "SOLUSDT": 150.0}
    now_ms = int(datetime.now(UTC).timestamp() * 1000)

    stream: list[dict[str, Any]] = []
    created_trades: list[dict[str, Any]] = []

    # 1. Generate INSERTS ('c')
    for i in range(1, insert_count + 1):
        sym = random.choice(symbols)
        price = round(base_prices[sym] * random.uniform(0.98, 1.02), 4)
        qty = round(random.uniform(0.01, 1.5), 4)
        quote_qty = round(price * qty, 4)
        t_id = 4000000 + i

        trade_row = {
            "trade_id": t_id,
            "symbol": sym,
            "price": price,
            "quantity": qty,
            "quote_quantity": quote_qty,
            "trade_timestamp": datetime.fromtimestamp(
                (now_ms - (insert_count - i) * 1000) / 1000.0, tz=UTC
            ).isoformat(),
            "is_buyer_maker": random.choice([True, False]),
            "record_hash": f"hash_{sym}_{t_id}",
        }
        created_trades.append(trade_row)

        stream.append(
            {
                "before": None,
                "after": trade_row,
                "source": {"version": "2.5.0", "connector": "postgresql", "lsn": 1000000 + i},
                "op": "c",
                "ts_ms": now_ms - (insert_count - i) * 1000,
            }
        )

    # 2. Generate UPDATES ('u') - e.g. Price adjustments on existing trades
    for i in range(update_count):
        target = random.choice(created_trades)
        before_state = dict(target)
        after_state = dict(target)
        # Modify price by 0.5%
        new_price = round(before_state["price"] * 1.005, 4)
        after_state["price"] = new_price
        after_state["quote_quantity"] = round(new_price * after_state["quantity"], 4)
        after_state["record_hash"] = (
            f"hash_{after_state['symbol']}_{after_state['trade_id']}_revised"
        )

        stream.append(
            {
                "before": before_state,
                "after": after_state,
                "source": {"version": "2.5.0", "connector": "postgresql", "lsn": 2000000 + i},
                "op": "u",
                "ts_ms": now_ms + (i * 500),
            }
        )

    # 3. Generate DELETES ('d') - e.g. Cancelled / rejected trades
    for i in range(delete_count):
        target = random.choice(created_trades)
        stream.append(
            {
                "before": dict(target),
                "after": None,
                "source": {"version": "2.5.0", "connector": "postgresql", "lsn": 3000000 + i},
                "op": "d",
                "ts_ms": now_ms + (i * 800),
            }
        )

    # 4. Inject POISON PILLS to test Data Quality contracts
    # Negative prices, null trade IDs, extreme price spikes
    for i in range(poison_pill_count):
        corrupt_type = i % 3
        if corrupt_type == 0:
            # Negative price
            bad_row = {
                "trade_id": 9999000 + i,
                "symbol": "BTCUSDT",
                "price": -500.0,
                "quantity": 1.0,
                "quote_quantity": -500.0,
                "trade_timestamp": datetime.now(UTC).isoformat(),
                "is_buyer_maker": True,
            }
        elif corrupt_type == 1:
            # Null trade ID
            bad_row = {
                "trade_id": None,
                "symbol": "ETHUSDT",
                "price": 3500.0,
                "quantity": 1.0,
                "quote_quantity": 3500.0,
                "trade_timestamp": datetime.now(UTC).isoformat(),
                "is_buyer_maker": False,
            }
        else:
            # Extreme price anomaly (fat-finger spike: 50x normal BTC price)
            bad_row = {
                "trade_id": 9999100 + i,
                "symbol": "BTCUSDT",
                "price": 3_500_000.0,  # Extreme anomaly
                "quantity": 0.1,
                "quote_quantity": 350_000.0,
                "trade_timestamp": datetime.now(UTC).isoformat(),
                "is_buyer_maker": False,
            }

        stream.append(
            {
                "before": None,
                "after": bad_row,
                "source": {"version": "2.5.0", "connector": "postgresql", "lsn": 4000000 + i},
                "op": "c",
                "ts_ms": now_ms + (i * 1000),
            }
        )

    return stream
