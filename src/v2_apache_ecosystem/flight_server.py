"""V2 Apache Arrow Flight High-Speed Zero-Copy gRPC Server.

Streams PyArrow RecordBatches directly from memory over gRPC port 8815,
eliminating serialization/deserialization (SerDe) bottlenecks.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
import pyarrow.flight as flight

logger = logging.getLogger("ArrowFlightServer")

CANDLE_SCHEMA = pa.schema([
    ("window_start", pa.string()),
    ("window_end", pa.string()),
    ("symbol", pa.string()),
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("volume", pa.float64()),
    ("vwap", pa.float64()),
    ("trade_count", pa.int64()),
])

TRADES_SCHEMA = pa.schema([
    ("trade_id", pa.string()),
    ("symbol", pa.string()),
    ("price", pa.float64()),
    ("volume", pa.float64()),
    ("timestamp", pa.timestamp("us", tz="UTC")),
    ("row_hash", pa.string()),
])


def generate_mock_datasets() -> tuple[pa.Table, pa.Table]:
    """Generate in-memory PyArrow datasets for Flight streaming."""
    now_iso = datetime.now(UTC).isoformat()
    candles_data = {
        "window_start": [now_iso, now_iso, now_iso],
        "window_end": [now_iso, now_iso, now_iso],
        "symbol": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
        "open": [64200.0, 3450.0, 150.0],
        "high": [64350.0, 3490.0, 155.0],
        "low": [64180.0, 3445.0, 149.5],
        "close": [64290.0, 3482.0, 152.4],
        "volume": [45.2, 310.5, 1420.0],
        "vwap": [64282.4, 3478.1, 152.1],
        "trade_count": [128, 94, 62],
    }
    candles_table = pa.Table.from_pydict(candles_data, schema=CANDLE_SCHEMA)

    # 1,000 mock trades
    trade_ids = [f"FLIGHT-TRD-{i:06d}" for i in range(1000)]
    syms = ["BTCUSDT" if i % 3 == 0 else "ETHUSDT" if i % 3 == 1 else "SOLUSDT" for i in range(1000)]
    prices = [64000.0 + (i % 500) * 0.5 for i in range(1000)]
    volumes = [0.1 + (i % 20) * 0.05 for i in range(1000)]
    timestamps = [datetime.now(UTC) for _ in range(1000)]
    hashes = [f"hash-{i}" for i in range(1000)]

    trades_table = pa.Table.from_pydict(
        {
            "trade_id": trade_ids,
            "symbol": syms,
            "price": prices,
            "volume": volumes,
            "timestamp": timestamps,
            "row_hash": hashes,
        },
        schema=TRADES_SCHEMA,
    )

    return candles_table, trades_table


class MarketFlightServer(flight.FlightServerBase):
    """High-speed gRPC Arrow Flight Server delivering real-time ledger & candle data."""

    def __init__(self, location: str = "grpc://127.0.0.1:8815", **kwargs: Any) -> None:
        super().__init__(location, **kwargs)
        self._location = location
        self._candles_table, self._trades_table = generate_mock_datasets()
        logger.info("MarketFlightServer initialized at %s", location)

    def list_flights(self, context: Any, criteria: bytes) -> list[flight.FlightInfo]:
        """List active data streams available for zero-copy subscription."""
        flights = []

        # Stream 1: Candlesticks
        cand_desc = flight.FlightDescriptor.for_path(b"CANDLES_1MIN_STREAM")
        cand_info = flight.FlightInfo(
            CANDLE_SCHEMA,
            cand_desc,
            [flight.FlightEndpoint(b"CANDLES_1MIN_STREAM", [self._location])],
            self._candles_table.num_rows,
            self._candles_table.nbytes,
        )
        flights.append(cand_info)

        # Stream 2: Full Trades Ledger
        trd_desc = flight.FlightDescriptor.for_path(b"LEDGER_TRADES_AUDIT")
        trd_info = flight.FlightInfo(
            TRADES_SCHEMA,
            trd_desc,
            [flight.FlightEndpoint(b"LEDGER_TRADES_AUDIT", [self._location])],
            self._trades_table.num_rows,
            self._trades_table.nbytes,
        )
        flights.append(trd_info)

        return flights

    def get_flight_info(self, context: Any, descriptor: flight.FlightDescriptor) -> flight.FlightInfo:
        """Get schema and endpoint metadata for a requested flight descriptor."""
        path_key = descriptor.path[0].decode("utf-8") if descriptor.path else ""

        if path_key == "CANDLES_1MIN_STREAM":
            return flight.FlightInfo(
                CANDLE_SCHEMA,
                descriptor,
                [flight.FlightEndpoint(b"CANDLES_1MIN_STREAM", [self._location])],
                self._candles_table.num_rows,
                self._candles_table.nbytes,
            )
        elif path_key == "LEDGER_TRADES_AUDIT":
            return flight.FlightInfo(
                TRADES_SCHEMA,
                descriptor,
                [flight.FlightEndpoint(b"LEDGER_TRADES_AUDIT", [self._location])],
                self._trades_table.num_rows,
                self._trades_table.nbytes,
            )
        else:
            raise flight.FlightNotFoundError(f"Flight stream '{path_key}' not found.")

    def do_get(self, context: Any, ticket: flight.Ticket) -> flight.RecordBatchStream:
        """Stream PyArrow RecordBatches directly with zero-copy memory transport."""
        ticket_str = ticket.ticket.decode("utf-8")

        if ticket_str == "CANDLES_1MIN_STREAM":
            return flight.RecordBatchStream(self._candles_table)
        elif ticket_str == "LEDGER_TRADES_AUDIT":
            return flight.RecordBatchStream(self._trades_table)
        else:
            raise flight.FlightNotFoundError(f"Unknown ticket: '{ticket_str}'")

    def update_candles(self, candles_table: pa.Table) -> None:
        """Update live candlestick buffer in memory."""
        self._candles_table = candles_table


def start_flight_server(port: int = 8815) -> MarketFlightServer:
    """Instantiate and start the Flight Server."""
    location = f"grpc://127.0.0.1:{port}"
    server = MarketFlightServer(location=location)
    return server


if __name__ == "__main__":
    server = start_flight_server()
    print("⚡ Arrow Flight Server active on grpc://127.0.0.1:8815")
    try:
        server.wait()
    except KeyboardInterrupt:
        print("Stopping Flight server...")
        server.shutdown()
