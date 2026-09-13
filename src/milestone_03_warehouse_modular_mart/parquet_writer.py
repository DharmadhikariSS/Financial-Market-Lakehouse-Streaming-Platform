"""Columnar Lake Storage & Partitioning Engine.

Converts streaming or batched trade records into Snappy-compressed Apache Parquet
files, organized according to a Hive-partitioned directory layout:
  data/lakehouse/raw/trades/year=YYYY/month=MM/day=DD/symbol=SYMBOL/batch_<uuid>.parquet
"""

import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from src.common.config import BASE_DIR
from src.common.logger import get_logger

logger = get_logger("milestone_03.parquet_writer")

DEFAULT_LAKEHOUSE_DIR = BASE_DIR / "data" / "lakehouse" / "raw" / "trades"

TRADE_PYARROW_SCHEMA = pa.schema(
    [
        ("trade_id", pa.int64()),
        ("symbol", pa.string()),
        ("price", pa.float64()),
        ("quantity", pa.float64()),
        ("quote_quantity", pa.float64()),
        ("trade_timestamp", pa.string()),
        ("is_buyer_maker", pa.bool_()),
        ("year", pa.int32()),
        ("month", pa.int32()),
        ("day", pa.int32()),
    ]
)


class ParquetLakeWriter:
    """Writes compressed, Hive-partitioned Parquet files to local lakehouse storage."""

    def __init__(self, base_output_dir: Path | None = None) -> None:
        self.base_dir = base_output_dir or DEFAULT_LAKEHOUSE_DIR
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def write_partitioned_trades(
        self,
        trades: list[dict[str, Any]],
        compression: str = "snappy",
    ) -> list[Path]:
        """Partitions trade records by year, month, day, and symbol, writing Snappy Parquet files."""
        if not trades:
            logger.warning("No records supplied to ParquetLakeWriter.")
            return []

        # Group records by partition tuple: (year, month, day, symbol)
        partitions: dict[tuple[int, int, int, str], list[dict[str, Any]]] = {}

        for t in trades:
            dt = datetime.fromisoformat(t["trade_timestamp"])
            part_key = (dt.year, dt.month, dt.day, t["symbol"].upper())

            if part_key not in partitions:
                partitions[part_key] = []

            record = dict(t)
            record["year"] = dt.year
            record["month"] = dt.month
            record["day"] = dt.day
            partitions[part_key].append(record)

        written_files: list[Path] = []

        for (year, month, day, symbol), rows in partitions.items():
            partition_dir = (
                self.base_dir
                / f"year={year}"
                / f"month={month:02d}"
                / f"day={day:02d}"
                / f"symbol={symbol}"
            )
            partition_dir.mkdir(parents=True, exist_ok=True)

            batch_id = str(uuid.uuid4())[:8]
            output_file = partition_dir / f"batch_{batch_id}.parquet"

            # Create PyArrow Table
            arrow_table = pa.Table.from_pylist(rows, schema=TRADE_PYARROW_SCHEMA)

            # Write Snappy Parquet file
            pq.write_table(
                arrow_table,
                output_file,
                compression=compression,
                use_dictionary=True,
            )

            file_size_kb = output_file.stat().st_size / 1024.0
            try:
                display_path = str(output_file.relative_to(BASE_DIR))
            except ValueError:
                display_path = str(output_file)

            logger.info(
                f"Wrote {len(rows)} records to {display_path} "
                f"({file_size_kb:.2f} KB, {compression.upper()} compression)"
            )
            written_files.append(output_file)

        return written_files

    def get_lakehouse_stats(self) -> dict[str, Any]:
        """Calculates storage footprint and file count across the lakehouse."""
        total_files = 0
        total_bytes = 0
        symbols_found = set()

        for p_file in self.base_dir.rglob("*.parquet"):
            total_files += 1
            total_bytes += p_file.stat().st_size
            for part in p_file.parts:
                if part.startswith("symbol="):
                    symbols_found.add(part.split("=")[1])

        return {
            "total_files": total_files,
            "total_size_mb": round(total_bytes / (1024 * 1024), 3),
            "symbols": sorted(symbols_found),
            "root_path": str(self.base_dir),
        }
