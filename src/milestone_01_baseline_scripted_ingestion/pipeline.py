"""Milestone 01: Baseline Scripted Ingestion Pipeline.

Features:
- Dual-target execution: PostgreSQL (Docker) or embedded DuckDB (zero-RAM/local).
- Streaming chunked processing (chunksize=25,000) guaranteeing execution under 100MB RAM.
- Strict schema coercion: symbol normalization, timestamp parsing, float sanitization.
- Deterministic SHA-256 surrogate hashing (record_hash).
- Atomic idempotent merge logic (ON CONFLICT DO UPDATE WHERE record_hash != EXCLUDED.record_hash).
- Comprehensive audit metrics logging (records scanned, upserted, duration, memory).
"""

import argparse
import csv
import hashlib
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.common.config import BASE_DIR, settings
from src.common.database import db_manager
from src.common.logger import get_logger

logger = get_logger("milestone_01.pipeline")


def compute_record_hash(
    symbol: str,
    trade_id: int,
    price: float,
    quantity: float,
    trade_timestamp: str,
    is_buyer_maker: bool,
) -> str:
    """Computes a deterministic SHA-256 hash representing record state."""
    raw_payload = (
        f"{symbol}|{trade_id}|{price:.8f}|{quantity:.8f}|{trade_timestamp}|{is_buyer_maker}"
    )
    return hashlib.sha256(raw_payload.encode("utf-8")).hexdigest()


def sanitize_row(raw_row: dict[str, Any]) -> dict[str, Any] | None:
    """Sanitizes, validates, and types a raw CSV row."""
    try:
        # 1. Symbol normalization: strip and uppercase
        raw_symbol = str(raw_row.get("symbol", "")).strip().upper()
        if not raw_symbol or len(raw_symbol) > 20:
            return None

        # 2. Primary Key validation
        trade_id = int(raw_row["trade_id"])
        if trade_id <= 0:
            return None

        # 3. Numeric coercions (handling scientific notation e.g. 6.45e4)
        price = float(raw_row["price"])
        quantity = float(raw_row["quantity"])
        if price <= 0 or quantity <= 0:
            return None

        quote_quantity = float(raw_row.get("quote_quantity") or (price * quantity))

        # 4. Timestamp normalization to UTC ISO-8601
        raw_ts = str(raw_row["trade_timestamp"]).strip()
        try:
            if raw_ts.isdigit():
                # Epoch millisecond handling
                epoch_sec = int(raw_ts) / 1000.0
                dt = datetime.fromtimestamp(epoch_sec, tz=UTC)
            else:
                # ISO string handling
                dt = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
            ts_str = dt.isoformat()
        except Exception:
            return None

        # 5. Boolean coercion
        raw_buyer = str(raw_row.get("is_buyer_maker", "false")).strip().lower()
        is_buyer_maker = raw_buyer in ("true", "1", "t", "yes")

        # 6. Surrogate Hash
        rec_hash = compute_record_hash(
            raw_symbol, trade_id, price, quantity, ts_str, is_buyer_maker
        )

        return {
            "symbol": raw_symbol,
            "trade_id": trade_id,
            "price": price,
            "quantity": quantity,
            "quote_quantity": quote_quantity,
            "trade_timestamp": ts_str,
            "is_buyer_maker": is_buyer_maker,
            "record_hash": rec_hash,
        }
    except (ValueError, KeyError, TypeError):
        return None


class IngestionPipeline:
    """Idempotent batch ingestion pipeline supporting PostgreSQL and DuckDB."""

    def __init__(self, target_engine: str | None = None) -> None:
        self.target_engine = target_engine or settings.DB_ENGINE
        self.db = db_manager
        self.db.engine_type = self.target_engine

    def initialize_schema(self) -> None:
        """Initializes database schema from schema.sql."""
        schema_path = Path(__file__).parent / "schema.sql"
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema definition not found at {schema_path}")

        logger.info(f"Applying relational schema on {self.target_engine}...")
        ddl = schema_path.read_text(encoding="utf-8")
        self.db.execute_ddl(ddl)

    def process_csv(self, file_path: Path, chunk_size: int = 25_000) -> dict[str, Any]:
        """Streams a CSV file in chunks and performs idempotent set-based upserts."""
        if not file_path.exists():
            raise FileNotFoundError(f"Target CSV file not found: {file_path}")

        batch_id = str(uuid.uuid4())
        start_time = time.time()
        total_scanned = 0
        total_valid = 0
        total_quarantined = 0

        logger.info(
            f"Starting ingestion: file={file_path.name}, target={self.target_engine}, batch_id={batch_id}"
        )

        chunk: list[dict[str, Any]] = []

        with open(file_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                total_scanned += 1
                sanitized = sanitize_row(row)
                if sanitized:
                    chunk.append(sanitized)
                    total_valid += 1
                else:
                    total_quarantined += 1

                if len(chunk) >= chunk_size:
                    self._flush_chunk(chunk, batch_id)
                    chunk.clear()

            if chunk:
                self._flush_chunk(chunk, batch_id)
                chunk.clear()

        duration = time.time() - start_time
        throughput = total_scanned / duration if duration > 0 else 0

        # Query total records in target fact table
        final_count = self._get_fact_count()

        audit_metrics = {
            "batch_id": batch_id,
            "target_engine": self.target_engine,
            "file": file_path.name,
            "total_scanned": total_scanned,
            "total_valid": total_valid,
            "total_quarantined": total_quarantined,
            "final_table_count": final_count,
            "duration_seconds": round(duration, 3),
            "records_per_second": round(throughput, 1),
        }

        logger.info(
            f"Ingestion complete: {total_valid} valid / {total_quarantined} quarantined / "
            f"{final_count} total stored in {audit_metrics['duration_seconds']}s "
            f"({audit_metrics['records_per_second']} rec/s)"
        )
        return audit_metrics

    def _flush_chunk(self, chunk: list[dict[str, Any]], batch_id: str) -> None:
        """Flushes a validated chunk to the database using an atomic idempotent upsert."""
        if not chunk:
            return

        with self.db.get_connection() as conn:
            if self.target_engine == "duckdb":
                self._flush_duckdb(conn, chunk)
            elif self.target_engine == "postgres":
                self._flush_postgres(conn, chunk, batch_id)

    def _flush_duckdb(self, conn: Any, chunk: list[dict[str, Any]]) -> None:
        """Executes atomic UPSERT against DuckDB."""
        # Convert chunk into DuckDB table view
        import pyarrow as pa

        arrow_table = pa.Table.from_pylist(chunk)
        conn.register("chunk_arrow_view", arrow_table)

        upsert_sql = """
        INSERT INTO core.fact_trades (
            symbol, trade_id, price, quantity, quote_quantity,
            trade_timestamp, is_buyer_maker, record_hash, created_at, updated_at
        )
        SELECT
            symbol,
            trade_id,

            price,
            quantity,
            quote_quantity,
            CAST(trade_timestamp AS TIMESTAMP WITH TIME ZONE),
            is_buyer_maker,
            record_hash,
            now(),
            now()
        FROM chunk_arrow_view
        ON CONFLICT (symbol, trade_id) DO UPDATE SET
            price = EXCLUDED.price,
            quantity = EXCLUDED.quantity,
            quote_quantity = EXCLUDED.quote_quantity,
            trade_timestamp = EXCLUDED.trade_timestamp,
            is_buyer_maker = EXCLUDED.is_buyer_maker,
            record_hash = EXCLUDED.record_hash,
            updated_at = now()
        WHERE core.fact_trades.record_hash != EXCLUDED.record_hash;
        """
        conn.execute(upsert_sql)
        conn.unregister("chunk_arrow_view")

    def _flush_postgres(self, conn: Any, chunk: list[dict[str, Any]], batch_id: str) -> None:
        """Executes staging-to-fact atomic UPSERT against PostgreSQL."""
        from sqlalchemy import text

        # 1. Bulk insert to staging table
        staging_rows = [
            {
                "symbol": r["symbol"],
                "trade_id": r["trade_id"],
                "price": r["price"],
                "quantity": r["quantity"],
                "quote_quantity": r["quote_quantity"],
                "trade_timestamp": r["trade_timestamp"],
                "is_buyer_maker": r["is_buyer_maker"],
                "record_hash": r["record_hash"],
                "batch_id": batch_id,
            }
            for r in chunk
        ]

        insert_stg_sql = text("""
        INSERT INTO staging.stg_raw_trades (
            symbol, trade_id, price, quantity, quote_quantity,
            trade_timestamp, is_buyer_maker, record_hash, batch_id
        ) VALUES (
            :symbol, :trade_id, :price, :quantity, :quote_quantity,
            CAST(:trade_timestamp AS TIMESTAMPTZ), :is_buyer_maker, :record_hash, :batch_id
        );
        """)
        conn.execute(insert_stg_sql, staging_rows)

        # 2. Atomic Set-Based Upsert from staging to core
        merge_sql = text("""
        INSERT INTO core.fact_trades (
            symbol, trade_id, price, quantity, quote_quantity,
            trade_timestamp, is_buyer_maker, record_hash, created_at, updated_at
        )
        SELECT
            symbol, trade_id, price, quantity, quote_quantity,

            trade_timestamp, is_buyer_maker, record_hash,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM staging.stg_raw_trades
        WHERE batch_id = :batch_id
        ON CONFLICT (symbol, trade_id) DO UPDATE SET
            price = EXCLUDED.price,
            quantity = EXCLUDED.quantity,
            quote_quantity = EXCLUDED.quote_quantity,
            trade_timestamp = EXCLUDED.trade_timestamp,
            is_buyer_maker = EXCLUDED.is_buyer_maker,
            record_hash = EXCLUDED.record_hash,
            updated_at = CURRENT_TIMESTAMP
        WHERE core.fact_trades.record_hash != EXCLUDED.record_hash;
        """)
        conn.execute(merge_sql, {"batch_id": batch_id})

        # 3. Clean up staging
        conn.execute(
            text("DELETE FROM staging.stg_raw_trades WHERE batch_id = :batch_id;"),
            {"batch_id": batch_id},
        )
        conn.commit()

    def _get_fact_count(self) -> int:
        """Queries the current number of rows in core.fact_trades."""
        with self.db.get_connection() as conn:
            if self.target_engine == "duckdb":
                res = conn.execute("SELECT COUNT(*) FROM core.fact_trades;").fetchone()
                return int(res[0]) if res else 0
            elif self.target_engine == "postgres":
                from sqlalchemy import text

                res = conn.execute(text("SELECT COUNT(*) FROM core.fact_trades;")).fetchone()
                return int(res[0]) if res else 0
        return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Milestone 01: Baseline Scripted Ingestion Pipeline"
    )
    parser.add_argument(
        "--file",
        type=str,
        default="data/unit_test_50.csv",
        help="Path to CSV file to ingest (default: data/unit_test_50.csv)",
    )
    parser.add_argument(
        "--target",
        type=str,
        choices=["duckdb", "postgres"],
        default=settings.DB_ENGINE,
        help="Target database engine (default: settings.DB_ENGINE)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=settings.BATCH_SIZE,
        help=f"Chunk size for stream processing (default: {settings.BATCH_SIZE})",
    )
    parser.add_argument(
        "--init-schema",
        action="store_true",
        help="Execute schema.sql to initialize or migrate target tables",
    )

    args = parser.parse_args()

    input_file = Path(args.file)
    if not input_file.is_absolute():
        input_file = BASE_DIR / input_file

    pipeline = IngestionPipeline(target_engine=args.target)

    # Initialize schema if requested or if DuckDB target file is fresh
    if args.init_schema or args.target == "duckdb":
        pipeline.initialize_schema()

    metrics = pipeline.process_csv(input_file, chunk_size=args.chunk_size)
    print("\n--- INGESTION AUDIT SUMMARY ---")
    for k, v in metrics.items():
        print(f"  {k:20}: {v}")


if __name__ == "__main__":
    main()
