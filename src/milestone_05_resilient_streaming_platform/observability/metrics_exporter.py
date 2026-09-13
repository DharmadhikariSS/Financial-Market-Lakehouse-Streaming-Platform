"""Prometheus Metrics Exporter for Streaming Resilience.

Exposes standard OpenMetrics format over HTTP endpoint /metrics (port 9102) for Prometheus
scraping. Tracks ingestion throughput, byte volume, watermark lag, consumer group lag,
and DLQ incident frequencies.
"""

from __future__ import annotations

import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, ClassVar

from src.common.logger import get_logger

logger = get_logger("metrics_exporter")


class StreamingMetricsRegistry:
    """In-memory registry tracking streaming operational indicators."""

    _instance: ClassVar[StreamingMetricsRegistry | None] = None

    def __init__(self) -> None:
        self.events_total: dict[tuple[str, str], int] = {}  # (symbol, status) -> count
        self.bytes_total: int = 0
        self.consumer_lag_records: int = 0
        self.watermark_lag_seconds: float = 5.0
        self.dlq_errors_total: dict[str, int] = {}  # error_code -> count
        self.windows_computed_total: int = 0
        self.start_time: float = time.time()

    @classmethod
    def get_instance(cls) -> StreamingMetricsRegistry:
        if cls._instance is None:
            cls._instance = StreamingMetricsRegistry()
        return cls._instance

    def inc_events(self, symbol: str, status: str = "success", count: int = 1) -> None:
        key = (symbol, status)
        self.events_total[key] = self.events_total.get(key, 0) + count

    def inc_bytes(self, count: int) -> None:
        self.bytes_total += count

    def set_watermark_lag(self, lag_sec: float) -> None:
        self.watermark_lag_seconds = max(0.0, lag_sec)

    def set_consumer_lag(self, lag_records: int) -> None:
        self.consumer_lag_records = max(0, lag_records)

    def inc_dlq(self, error_code: str, count: int = 1) -> None:
        self.dlq_errors_total[error_code] = self.dlq_errors_total.get(error_code, 0) + count

    def inc_windows(self, count: int = 1) -> None:
        self.windows_computed_total += count

    def generate_metrics_text(self) -> str:
        """Generate Prometheus exposition text format."""
        lines = [
            "# HELP streaming_events_total Total number of stream events processed",
            "# TYPE streaming_events_total counter",
        ]
        if not self.events_total:
            lines.append('streaming_events_total{symbol="ALL",status="none"} 0')
        else:
            for (sym, stat), cnt in self.events_total.items():
                lines.append(f'streaming_events_total{{symbol="{sym}",status="{stat}"}} {cnt}')

        lines.extend(
            [
                "# HELP streaming_bytes_total Total raw bytes processed by stream engine",
                "# TYPE streaming_bytes_total counter",
                f"streaming_bytes_total {self.bytes_total}",
                "# HELP streaming_consumer_lag_records Current unconsumed backlog offset count",
                "# TYPE streaming_consumer_lag_records gauge",
                f"streaming_consumer_lag_records {self.consumer_lag_records}",
                "# HELP streaming_watermark_lag_seconds Event-time delay configured on watermark",
                "# TYPE streaming_watermark_lag_seconds gauge",
                f"streaming_watermark_lag_seconds {self.watermark_lag_seconds}",
                "# HELP streaming_dlq_errors_total Total poison-pill events routed to DLQ",
                "# TYPE streaming_dlq_errors_total counter",
            ]
        )
        if not self.dlq_errors_total:
            lines.append('streaming_dlq_errors_total{error_code="NONE"} 0')
        else:
            for err, cnt in self.dlq_errors_total.items():
                lines.append(f'streaming_dlq_errors_total{{error_code="{err}"}} {cnt}')

        lines.extend(
            [
                "# HELP streaming_windows_computed_total Total tumbling candle windows materialized",
                "# TYPE streaming_windows_computed_total counter",
                f"streaming_windows_computed_total {self.windows_computed_total}",
            ]
        )
        return "\n".join(lines) + "\n"


class PrometheusMetricsHandler(BaseHTTPRequestHandler):
    """Simple HTTP request handler serving /metrics endpoint."""

    def do_GET(self) -> None:
        if self.path == "/metrics" or self.path == "/":
            body = StreamingMetricsRegistry.get_instance().generate_metrics_text().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        # Suppress verbose HTTP server access logs
        pass


def start_metrics_server(port: int = 9102) -> HTTPServer:
    """Start metrics HTTP server on background daemon thread."""
    server = HTTPServer(("0.0.0.0", port), PrometheusMetricsHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    logger.info(f"Prometheus metrics endpoint live on http://0.0.0.0:{port}/metrics")
    return server
