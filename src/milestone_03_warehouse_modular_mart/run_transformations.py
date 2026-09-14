"""Analytical Transformation Pipeline Runner.

Executes Kimball dimensional modeling across the data lakehouse.
Provides native DuckDB SQL execution for immediate zero-dependency reproduction,
with automatic fallback/detection for 'dbt build'.
"""

import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

import duckdb

from src.common.config import BASE_DIR
from src.common.logger import get_logger

logger = get_logger("milestone_03.run_transformations")

DEFAULT_DB_PATH = BASE_DIR / "data" / "lakehouse" / "warehouse.duckdb"
DEFAULT_PARQUET_GLOB = "data/lakehouse/raw/trades/**/*.parquet"


class LakehouseTransformer:
    """Executes staging, intermediate, and mart dimensional models on DuckDB."""

    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = db_path or DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def run_dbt_cli(self) -> bool:
        """Attempts to execute models via dbt CLI if installed."""
        dbt_exe = shutil.which("dbt")
        if not dbt_exe:
            logger.info("dbt CLI not found in PATH; utilizing native DuckDB transformation runner.")
            return False

        logger.info("Executing transformations via dbt CLI...")
        try:
            res = subprocess.run(
                [dbt_exe, "build", "--profiles-dir", "."],
                cwd=str(BASE_DIR / "dbt_project"),
                capture_output=True,
                text=True,
                check=False,
            )
            print(res.stdout)
            if res.returncode == 0:
                logger.info("dbt build completed successfully.")
                return True
            logger.warning(f"dbt build returned exit code {res.returncode}: {res.stderr}")
            return False
        except Exception as err:
            logger.warning(f"Failed to invoke dbt: {err}")
            return False

    def run_native_duckdb(self) -> dict[str, Any]:
        """Executes the exact dimensional star schema DAG natively inside DuckDB."""
        start_time = time.time()
        logger.info(f"Opening DuckDB warehouse at: {self.db_path}")

        conn = duckdb.connect(str(self.db_path))

        try:
            # 1. Schemas
            conn.execute("CREATE SCHEMA IF NOT EXISTS staging;")
            conn.execute("CREATE SCHEMA IF NOT EXISTS intermediate;")
            conn.execute("CREATE SCHEMA IF NOT EXISTS marts;")

            # 2. Check if parquet files exist
            parquet_files = list(BASE_DIR.glob(DEFAULT_PARQUET_GLOB))
            if not parquet_files:
                raise FileNotFoundError(
                    f"No raw Parquet files found under {DEFAULT_PARQUET_GLOB}. "
                    "Run extract_api.py or parquet_writer.py first."
                )

            logger.info(f"Discovered {len(parquet_files)} raw Parquet files in lakehouse.")

            # 3. Layer 1: Staging View
            logger.info("Materializing Layer 1: staging.stg_market_trades...")
            glob_path = str(BASE_DIR / DEFAULT_PARQUET_GLOB).replace("\\", "/")
            conn.execute(f"""
            CREATE OR REPLACE VIEW staging.stg_market_trades AS
            SELECT
                trade_id,
                UPPER(TRIM(symbol)) AS symbol,
                CAST(price AS DOUBLE) AS price,
                CAST(quantity AS DOUBLE) AS quantity,
                CAST(quote_quantity AS DOUBLE) AS quote_quantity,
                CAST(trade_timestamp AS TIMESTAMP WITH TIME ZONE) AS trade_timestamp,
                CAST(is_buyer_maker AS BOOLEAN) AS is_buyer_maker,
                CAST(year AS INTEGER) AS partition_year,
                CAST(month AS INTEGER) AS partition_month,
                CAST(day AS INTEGER) AS partition_day
            FROM read_parquet('{glob_path}', hive_partitioning = 1)
            WHERE trade_id IS NOT NULL AND price > 0 AND quantity > 0;
            """)

            # 4. Layer 2: Intermediate Deduplicated View
            logger.info("Materializing Layer 2: intermediate.int_trades_deduped...")
            conn.execute("""
            CREATE OR REPLACE VIEW intermediate.int_trades_deduped AS
            WITH ranked AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol, trade_id
                        ORDER BY trade_timestamp DESC
                    ) AS row_num
                FROM staging.stg_market_trades
            )
            SELECT
                md5(symbol || '-' || CAST(trade_id AS VARCHAR)) AS trade_surrogate_key,
                trade_id,
                symbol,
                price,
                quantity,
                quote_quantity,
                trade_timestamp,
                is_buyer_maker,
                CAST(strftime(trade_timestamp, '%Y%m%d') AS INTEGER) AS date_key,
                partition_year,
                partition_month,
                partition_day
            FROM ranked
            WHERE row_num = 1;
            """)

            # 5. Layer 3: Marts - dim_assets (Table)
            logger.info("Materializing Layer 3: marts.dim_assets...")
            conn.execute("""
            CREATE OR REPLACE TABLE marts.dim_assets AS
            WITH distinct_symbols AS (
                SELECT DISTINCT symbol FROM intermediate.int_trades_deduped
            )
            SELECT
                md5(symbol) AS asset_key,
                symbol,
                CASE
                    WHEN symbol LIKE '%USDT' THEN REPLACE(symbol, 'USDT', '')
                    WHEN symbol LIKE '%USDC' THEN REPLACE(symbol, 'USDC', '')
                    WHEN symbol LIKE '%BTC' THEN REPLACE(symbol, 'BTC', '')
                    ELSE symbol
                END AS base_asset,
                CASE
                    WHEN symbol LIKE '%USDT' THEN 'USDT'
                    WHEN symbol LIKE '%USDC' THEN 'USDC'
                    WHEN symbol LIKE '%BTC' THEN 'BTC'
                    ELSE 'UNKNOWN'
                END AS quote_asset,
                'CRYPTOCURRENCY' AS asset_class,
                TRUE AS is_active,
                CURRENT_TIMESTAMP AS effective_timestamp
            FROM distinct_symbols;
            """)

            # 6. Layer 3: Marts - fct_trades (Table)
            logger.info("Materializing Layer 3: marts.fct_trades...")
            conn.execute("""
            CREATE OR REPLACE TABLE marts.fct_trades AS
            SELECT
                t.trade_surrogate_key,
                t.trade_id,
                a.asset_key,
                t.symbol,
                t.price,
                t.quantity,
                t.quote_quantity,
                t.trade_timestamp,
                t.is_buyer_maker,
                t.date_key,
                t.partition_year,
                t.partition_month,
                t.partition_day
            FROM intermediate.int_trades_deduped t
            INNER JOIN marts.dim_assets a ON t.symbol = a.symbol;
            """)

            # 7. Layer 3: Marts - agg_daily_market_metrics (Table)
            logger.info(
                "Materializing Layer 3: marts.agg_daily_market_metrics (VWAP computation)..."
            )
            conn.execute("""
            CREATE OR REPLACE TABLE marts.agg_daily_market_metrics AS
            SELECT
                symbol,
                CAST(trade_timestamp AS DATE) AS trade_date,

                COUNT(*) AS total_trades,
                ROUND(SUM(quantity), 6) AS total_base_volume,
                ROUND(SUM(quote_quantity), 2) AS total_quote_volume,
                ROUND(MIN(price), 4) AS low_price,
                ROUND(MAX(price), 4) AS high_price,
                ROUND(SUM(price * quantity) / NULLIF(SUM(quantity), 0), 4) AS vwap,
                COUNT(*) FILTER (WHERE is_buyer_maker = TRUE) AS buyer_maker_trades,
                COUNT(*) FILTER (WHERE is_buyer_maker = FALSE) AS buyer_taker_trades,
                ROUND(
                    COUNT(*) FILTER (WHERE is_buyer_maker = FALSE) * 100.0 / NULLIF(COUNT(*), 0),
                    2
                ) AS taker_buy_percentage
            FROM marts.fct_trades
            GROUP BY symbol, CAST(trade_timestamp AS DATE)
            ORDER BY trade_date DESC, total_quote_volume DESC;
            """)

            # 8. Assertions & Audit Quality Gates
            dim_row = conn.execute("SELECT COUNT(*) FROM marts.dim_assets;").fetchone()
            dim_count = dim_row[0] if dim_row else 0
            fct_row = conn.execute("SELECT COUNT(*) FROM marts.fct_trades;").fetchone()
            fct_count = fct_row[0] if fct_row else 0
            agg_row = conn.execute(
                "SELECT COUNT(*) FROM marts.agg_daily_market_metrics;"
            ).fetchone()
            agg_count = agg_row[0] if agg_row else 0

            # Referential integrity test
            orphan_row = conn.execute("""
            SELECT COUNT(*) FROM marts.fct_trades f
            LEFT JOIN marts.dim_assets a ON f.asset_key = a.asset_key
            WHERE a.asset_key IS NULL;
            """).fetchone()
            orphan_count = orphan_row[0] if orphan_row else 0
            if orphan_count > 0:
                raise ValueError(
                    f"Referential integrity failure: {orphan_count} orphan trades found."
                )

            duration = round(time.time() - start_time, 3)

            results = {
                "status": "SUCCESS",
                "dim_assets_rows": dim_count,
                "fct_trades_rows": fct_count,
                "agg_daily_metrics_rows": agg_count,
                "orphan_records": orphan_count,
                "duration_seconds": duration,
                "db_path": str(self.db_path),
            }

            logger.info(
                f"Transformations successful: dim_assets={dim_count}, fct_trades={fct_count}, "
                f"agg_metrics={agg_count} in {duration}s."
            )
            return results

        finally:
            conn.close()


def main() -> None:
    transformer = LakehouseTransformer()
    # Attempt dbt CLI first; fallback to native runner
    if not transformer.run_dbt_cli():
        metrics = transformer.run_native_duckdb()
        print("\n--- TRANSFORMATION AUDIT SUMMARY ---")
        for k, v in metrics.items():
            print(f"  {k:25}: {v}")


if __name__ == "__main__":
    main()
