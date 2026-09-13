"""Apache Iceberg ACID Lakehouse Table & Merge Engine.

Implements ACID table transactions, row-level mutations (inserts, updates, deletes),
snapshot log versioning, and historical Time-Travel query capabilities.
"""

import json
import time
import uuid
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from src.common.config import BASE_DIR
from src.common.logger import get_logger
from src.milestone_04_lakehouse_cdc_contracts.cdc_consumer import CdcMutationEvent

logger = get_logger("milestone_04.iceberg_writer")

DEFAULT_ICEBERG_DIR = BASE_DIR / "data" / "lakehouse" / "iceberg"


class IcebergLakehouseTable:
    """Manages an Apache Iceberg-compatible ACID lakehouse table with snapshot versioning."""

    def __init__(self, table_dir: Path | None = None) -> None:
        self.table_dir = table_dir or DEFAULT_ICEBERG_DIR
        self.data_dir = self.table_dir / "data"
        self.metadata_dir = self.table_dir / "metadata"

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)

        self.table_uuid = str(uuid.uuid4())
        self._init_metadata_if_needed()

    def _init_metadata_if_needed(self) -> None:
        """Initializes table metadata v1 if no prior version exists."""
        versions = sorted(self.metadata_dir.glob("v*.metadata.json"))
        if not versions:
            metadata = {
                "format_version": 2,
                "table_uuid": self.table_uuid,
                "location": str(self.table_dir),
                "last_sequence_number": 0,
                "last_updated_ms": int(time.time() * 1000),
                "current_snapshot_id": -1,
                "snapshots": [],
                "snapshot_log": [],
            }
            meta_file = self.metadata_dir / "v1.metadata.json"
            meta_file.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
            logger.info("Initialized new Iceberg Lakehouse table metadata v1.")

    def get_latest_metadata(self) -> dict[str, Any]:
        """Returns the current table metadata dictionary."""
        versions = sorted(self.metadata_dir.glob("v*.metadata.json"))
        if not versions:
            raise FileNotFoundError("No metadata found.")
        latest_file = versions[-1]
        return json.loads(latest_file.read_text(encoding="utf-8"))

    def get_current_snapshot_id(self) -> int:
        """Returns the current active snapshot ID."""
        meta = self.get_latest_metadata()
        return meta.get("current_snapshot_id", -1)

    def apply_cdc_mutations(
        self,
        mutations: list[CdcMutationEvent],
    ) -> dict[str, Any]:
        """Applies a batch of CDC mutations (inserts, updates, deletes) in an atomic ACID transaction."""
        if not mutations:
            return {"status": "SKIPPED", "mutations_applied": 0}

        latest_meta = self.get_latest_metadata()
        current_version = len(list(self.metadata_dir.glob("v*.metadata.json")))
        new_version = current_version + 1

        # Load existing active state
        current_data = self._read_active_records()

        # Track mutation counts
        added_count = 0
        updated_count = 0
        deleted_count = 0

        # Apply mutations against (symbol, trade_id) primary key
        for m in mutations:
            key = (m.symbol, m.trade_id)

            if m.op_type == "insert":
                current_data[key] = m.payload
                added_count += 1

            elif m.op_type == "update":
                if key in current_data:
                    current_data[key] = m.payload
                    updated_count += 1
                else:
                    # Upsert behavior if record did not exist
                    current_data[key] = m.payload
                    added_count += 1

            elif m.op_type == "delete":
                if key in current_data:
                    del current_data[key]
                    deleted_count += 1

        # Write new immutable Parquet data file for this snapshot
        snapshot_id = int(time.time() * 1000)
        snapshot_file = self.data_dir / f"snap_{snapshot_id}.parquet"

        records_list = list(current_data.values())
        if records_list:
            arrow_table = pa.Table.from_pylist(records_list)
            pq.write_table(arrow_table, snapshot_file, compression="snappy")
        else:
            # Empty table placeholder
            arrow_table = pa.Table.from_pylist([{"trade_id": -1, "symbol": "DUMMY"}])
            pq.write_table(arrow_table, snapshot_file, compression="snappy")

        # Construct new snapshot metadata entry
        snapshot_entry = {
            "snapshot_id": snapshot_id,
            "timestamp_ms": int(time.time() * 1000),
            "manifest_file": str(snapshot_file),
            "summary": {
                "operation": "merge",
                "added_records": added_count,
                "updated_records": updated_count,
                "deleted_records": deleted_count,
                "total_records": len(current_data),
            },
        }

        latest_meta["last_updated_ms"] = snapshot_entry["timestamp_ms"]
        latest_meta["current_snapshot_id"] = snapshot_id
        latest_meta["snapshots"].append(snapshot_entry)
        latest_meta["snapshot_log"].append(
            {
                "timestamp_ms": snapshot_entry["timestamp_ms"],
                "snapshot_id": snapshot_id,
            }
        )

        # Atomic commit of new metadata version
        new_meta_file = self.metadata_dir / f"v{new_version}.metadata.json"
        new_meta_file.write_text(json.dumps(latest_meta, indent=2), encoding="utf-8")

        logger.info(
            f"ACID Commit v{new_version}: Snapshot {snapshot_id} committed "
            f"(+{added_count}, ~{updated_count}, -{deleted_count} -> Total: {len(current_data)} rows)"
        )

        return {
            "version": new_version,
            "snapshot_id": snapshot_id,
            "added": added_count,
            "updated": updated_count,
            "deleted": deleted_count,
            "total_records": len(current_data),
        }

    def _read_active_records(self) -> dict[tuple[str, int], dict[str, Any]]:
        """Reads records from the current active snapshot."""
        meta = self.get_latest_metadata()
        curr_snap_id = meta.get("current_snapshot_id", -1)
        if curr_snap_id == -1 or not meta["snapshots"]:
            return {}

        active_snap = next(s for s in meta["snapshots"] if s["snapshot_id"] == curr_snap_id)
        data_file = Path(active_snap["manifest_file"])
        if not data_file.exists():
            return {}

        table = pq.read_table(data_file)
        data = {}
        for row in table.to_pylist():
            if row.get("trade_id") == -1 and row.get("symbol") == "DUMMY":
                continue
            key = (row["symbol"], row["trade_id"])
            data[key] = row
        return data

    def time_travel_query(self, snapshot_id: int) -> list[dict[str, Any]]:
        """Executes a Time Travel query retrieving table state as of a historical snapshot ID."""
        meta = self.get_latest_metadata()
        match = next(
            (s for s in meta["snapshots"] if s["snapshot_id"] == snapshot_id),
            None,
        )
        if not match:
            raise ValueError(f"Snapshot ID {snapshot_id} not found in table history.")

        data_file = Path(match["manifest_file"])
        if not data_file.exists():
            raise FileNotFoundError(f"Data file for snapshot {snapshot_id} missing.")

        table = pq.read_table(data_file)
        records = [
            r
            for r in table.to_pylist()
            if not (r.get("trade_id") == -1 and r.get("symbol") == "DUMMY")
        ]
        logger.info(f"Time Travel query to snapshot {snapshot_id} returned {len(records)} records.")
        return records

    def list_snapshots(self) -> list[dict[str, Any]]:
        """Returns all historical snapshots."""
        meta = self.get_latest_metadata()
        return meta.get("snapshots", [])


# Backward-compatible alias
IcebergTableManager = IcebergLakehouseTable
