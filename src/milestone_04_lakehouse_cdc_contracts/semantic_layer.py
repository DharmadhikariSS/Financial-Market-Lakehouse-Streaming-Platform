"""Semantic Query Layer for downstream AI agents and analytics.

Exposes structured, pre-aggregated business metrics and audit lineage
over the Apache Iceberg lakehouse tables using DuckDB.
"""

from pathlib import Path
from typing import Any

import duckdb

from src.common.logger import get_logger
from src.milestone_04_lakehouse_cdc_contracts.iceberg_writer import IcebergLakehouseTable

logger = get_logger("milestone_04.semantic_layer")


class SemanticQueryLayer:
    """Provides analytical and governance interfaces over the Iceberg Lakehouse."""

    def __init__(self, lakehouse_table: IcebergLakehouseTable | None = None) -> None:
        self.table = lakehouse_table or IcebergLakehouseTable()

    def get_query_connection(self) -> duckdb.DuckDBPyConnection:
        """Returns a DuckDB in-memory connection with registered semantic views."""
        conn = duckdb.connect()

        # Get latest active snapshot file
        meta = self.table.get_latest_metadata()
        curr_snap_id = meta.get("current_snapshot_id", -1)

        if curr_snap_id != -1 and meta.get("snapshots"):
            active_snap = next(s for s in meta["snapshots"] if s["snapshot_id"] == curr_snap_id)
            data_file = Path(active_snap["manifest_file"])
            file_path = str(data_file).replace("\\", "/")

            # Semantic View 1: Active Trade Ledger
            conn.execute(f"""
            CREATE OR REPLACE VIEW v_realtime_trades AS
            SELECT
                trade_id,
                symbol,
                price,
                quantity,
                quote_quantity,
                CAST(trade_timestamp AS TIMESTAMP WITH TIME ZONE) AS trade_timestamp,
                is_buyer_maker,
                record_hash
            FROM read_parquet('{file_path}')
            WHERE trade_id > 0;
            """)

            # Semantic View 2: Pre-Aggregated Daily Metrics & VWAP
            conn.execute("""
            CREATE OR REPLACE VIEW v_daily_market_metrics AS
            SELECT
                symbol,
                CAST(trade_timestamp AS DATE) AS trade_date,
                COUNT(*) AS total_trades,
                ROUND(SUM(quantity), 4) AS base_volume,
                ROUND(SUM(quote_quantity), 2) AS quote_volume,
                ROUND(MIN(price), 2) AS low_price,
                ROUND(MAX(price), 2) AS high_price,
                ROUND(SUM(price * quantity) / NULLIF(SUM(quantity), 0), 4) AS vwap,
                ROUND(
                    COUNT(*) FILTER (WHERE is_buyer_maker = FALSE) * 100.0 / NULLIF(COUNT(*), 0),
                    2
                ) AS taker_buy_ratio
            FROM v_realtime_trades
            GROUP BY symbol, CAST(trade_timestamp AS DATE);
            """)

        # Semantic View 3: Governance & Lakehouse Snapshot History
        snapshots = self.table.list_snapshots()
        if snapshots:
            snap_records = [
                {
                    "snapshot_id": s["snapshot_id"],
                    "timestamp_utc": str(s["timestamp_ms"]),
                    "operation": s["summary"]["operation"],
                    "added_records": s["summary"]["added_records"],
                    "updated_records": s["summary"]["updated_records"],
                    "deleted_records": s["summary"]["deleted_records"],
                    "total_records": s["summary"]["total_records"],
                }
                for s in snapshots
            ]
            import pyarrow as pa

            conn.register("snap_arrow_view", pa.Table.from_pylist(snap_records))
            conn.execute("""
            CREATE OR REPLACE VIEW v_lakehouse_snapshots AS
            SELECT * FROM snap_arrow_view ORDER BY snapshot_id DESC;
            """)

        return conn

    def query_view(self, view_name: str) -> list[dict[str, Any]]:
        """Executes a query against a registered semantic view."""
        conn = self.get_query_connection()
        try:
            res = conn.execute(f"SELECT * FROM {view_name};").df()
            return res.to_dict(orient="records")
        finally:
            conn.close()


def main() -> None:
    semantic = SemanticQueryLayer()
    try:
        metrics = semantic.query_view("v_daily_market_metrics")
        print("\n--- SEMANTIC LAYER: DAILY METRICS & VWAP ---")
        for row in metrics:
            print(
                f"  {row['symbol']} on {row['trade_date']}: VWAP=${row['vwap']}, Vol=${row['quote_volume']:,} ({row['total_trades']} trades)"
            )
    except Exception as err:
        print(f"Semantic query preview: {err}")


if __name__ == "__main__":
    main()
