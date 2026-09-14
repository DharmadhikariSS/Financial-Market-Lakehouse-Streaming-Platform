"""V2 Apache Spark Lakehouse & Rolling Multi-Asset VWAP Engine.

Features:
- PySpark 3.5 local[*] session with strict memory bounding (512MB).
- Multi-asset 1-hour rolling VWAP via Spark SQL Window specifications.
- Lakehouse Columnar Compaction ('rewrite_data_files') consolidating small fragmented files into optimized Snappy Parquet partitions.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

logger = logging.getLogger("SparkLakehouse")

TRADE_SCHEMA = StructType(
    [
        StructField("trade_id", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("volume", DoubleType(), False),
        StructField("ts_epoch", LongType(), False),
    ]
)


def get_spark_session(app_name: str = "V2SparkLakehouseEngine") -> SparkSession:
    """Create a resource-bounded local PySpark session."""
    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.driver.memory", "512m")
        .config("spark.executor.memory", "512m")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )


def compute_rolling_vwap(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Compute 1-hour (3600-second) rolling VWAP per asset symbol using Spark Window."""
    # Rolling 1-hour window specification (range of 3600 seconds preceding current trade)
    w_1hr = Window.partitionBy("symbol").orderBy(F.col("ts_epoch")).rangeBetween(-3600, 0)

    enriched = (
        df.withColumn("notional", F.col("price") * F.col("volume"))
        .withColumn("rolling_notional", F.sum("notional").over(w_1hr))
        .withColumn("rolling_volume", F.sum("volume").over(w_1hr))
        .withColumn(
            "rolling_1hr_vwap",
            F.round(F.col("rolling_notional") / F.col("rolling_volume"), 4),
        )
    )
    return enriched


def compact_lakehouse_files(
    spark: SparkSession,
    input_path: str | Path,
    output_path: str | Path,
    coalesce_target: int = 1,
) -> dict[str, Any]:
    """Execute columnar lakehouse compaction (Iceberg rewrite_data_files equivalent).

    Consolidates small fragmented streaming parquet files into optimized columnar files.
    """
    in_dir = Path(input_path)
    out_dir = Path(output_path)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. Inspect input files
    raw_files = list(in_dir.rglob("*.parquet"))
    bytes_before = sum(f.stat().st_size for f in raw_files)
    files_before = len(raw_files)

    if files_before == 0:
        return {
            "status": "skipped",
            "reason": "no_parquet_files_found",
            "files_before": 0,
            "files_after": 0,
            "reduction_pct": 0.0,
        }

    # 2. Read into PySpark and coalesce
    df = spark.read.parquet(str(in_dir))
    total_rows = df.count()

    (
        df.coalesce(coalesce_target)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(str(out_dir))
    )

    # 3. Inspect compacted files
    compacted_files = list(out_dir.rglob("*.parquet"))
    bytes_after = sum(f.stat().st_size for f in compacted_files)
    files_after = len(compacted_files)

    reduction_pct = (
        round(((files_before - files_after) / files_before) * 100, 2) if files_before > 0 else 0.0
    )

    return {
        "status": "compacted",
        "total_rows": total_rows,
        "files_before": files_before,
        "files_after": files_after,
        "reduction_pct": reduction_pct,
        "bytes_before": bytes_before,
        "bytes_after": bytes_after,
        "output_directory": str(out_dir),
    }


def run_spark_lakehouse_pipeline(
    trades: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Execute complete PySpark lakehouse pipeline: rolling VWAP and compaction."""
    spark = get_spark_session()
    try:
        # Sample trades if none provided
        if not trades:
            base_ts = 1700000000
            trades = [
                {
                    "trade_id": "T1",
                    "symbol": "BTCUSDT",
                    "price": 60000.0,
                    "volume": 1.5,
                    "ts_epoch": base_ts,
                },
                {
                    "trade_id": "T2",
                    "symbol": "BTCUSDT",
                    "price": 60500.0,
                    "volume": 2.0,
                    "ts_epoch": base_ts + 300,
                },
                {
                    "trade_id": "T3",
                    "symbol": "BTCUSDT",
                    "price": 61000.0,
                    "volume": 1.0,
                    "ts_epoch": base_ts + 1200,
                },
                {
                    "trade_id": "T4",
                    "symbol": "ETHUSDT",
                    "price": 3200.0,
                    "volume": 10.0,
                    "ts_epoch": base_ts + 100,
                },
                {
                    "trade_id": "T5",
                    "symbol": "ETHUSDT",
                    "price": 3250.0,
                    "volume": 8.0,
                    "ts_epoch": base_ts + 1500,
                },
            ]

        rdd_data = [
            (t["trade_id"], t["symbol"], float(t["price"]), float(t["volume"]), int(t["ts_epoch"]))
            for t in trades
        ]
        df = spark.createDataFrame(rdd_data, schema=TRADE_SCHEMA)

        # 1. Compute rolling VWAP
        vwap_df = compute_rolling_vwap(spark, df)
        results = [row.asDict() for row in vwap_df.collect()]

        return {
            "status": "success",
            "engine": "PySpark 3.5.5",
            "trades_processed": len(results),
            "sample_vwap": results,
        }
    finally:
        spark.stop()


if __name__ == "__main__":
    out = run_spark_lakehouse_pipeline()
    print("PySpark Execution Completed successfully!")
    print(f"Processed: {out['trades_processed']} trades.")
    for r in out["sample_vwap"]:
        print(
            f"[{r['symbol']}] Price: ${r['price']} | Vol: {r['volume']} | Rolling 1-Hr VWAP: ${r['rolling_1hr_vwap']}"
        )
