"""V2 Enterprise Apache Data Stack.

Enterprise-grade streaming and lakehouse architecture using:
- Apache Beam (Unified streaming with tumbling windows & tagged DLQ side-outputs)
- Apache Spark 3.5 (Rolling multi-asset VWAP and columnar compaction)
- PyIceberg 0.12 (Official table catalog with hidden partitioning & ACID metadata)
- Apache Arrow Flight (High-speed zero-copy gRPC data streaming)
"""

__all__ = [
    "beam_pipeline",
    "spark_lakehouse",
    "pyiceberg_catalog",
    "flight_server",
    "flight_client",
]
