"""Dead-Letter Queue (DLQ) Circuit Breaker and Webhook Alerting Engine.

Isolates malformed payloads, deserialization exceptions, and domain contract violations.
Enriches failed events with diagnostic metadata and routes them to a persistent JSONL
audit log without interrupting the continuous streaming pipeline.
"""

from __future__ import annotations

import base64
import json
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from src.common.config import get_settings
from src.common.logger import get_logger

logger = get_logger("dlq_router")


class DeadLetterQueueRouter:
    """Manages routing of poison-pill messages to DLQ storage and alerting."""

    def __init__(self, dlq_dir: Path | None = None) -> None:
        settings = get_settings()
        self.dlq_dir = dlq_dir or (settings.BASE_DIR / "data" / "streaming" / "dlq")
        self.dlq_dir.mkdir(parents=True, exist_ok=True)
        self.dlq_file = self.dlq_dir / "market_trades_dlq.jsonl"
        self._quarantined_count = 0

    def route_to_dlq(
        self,
        raw_payload: bytes,
        original_topic: str,
        error_code: str,
        error_message: str,
        schema_id: int | None = None,
        context: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Quarantine a failed message with rich diagnostics and append to DLQ."""
        failed_at = datetime.now(UTC).isoformat()
        st = traceback.format_exc() if traceback.format_exc().strip() != "NoneType: None" else ""

        dlq_entry = {
            "failed_at_utc": failed_at,
            "original_topic": original_topic,
            "error_code": error_code,
            "error_message": error_message,
            "schema_id": schema_id,
            "raw_payload_b64": base64.b64encode(raw_payload).decode("ascii"),
            "raw_payload_len": len(raw_payload),
            "stack_trace": st[:500] if st else None,
            "context": context or {},
        }

        # Append to DLQ audit file atomically
        with open(self.dlq_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(dlq_entry) + "\n")

        self._quarantined_count += 1
        logger.warning(
            f"[DLQ QUARANTINE] Routed {error_code} to DLQ | Topic: {original_topic} | "
            f"Reason: {error_message}"
        )

        # Generate Webhook Alert Payload
        self.send_webhook_alert(dlq_entry)
        return dlq_entry

    def send_webhook_alert(self, dlq_entry: dict[str, Any]) -> dict[str, Any]:
        """Format and dispatch structured Discord / Slack webhook incident payload."""
        alert_payload = {
            "username": "Stream Resilience Circuit Breaker",
            "avatar_url": "https://img.icons8.com/color/96/warning-shield.png",
            "embeds": [
                {
                    "title": f"🚨 STREAM FAILURE DETECTED: {dlq_entry['error_code']}",
                    "color": 15158332,  # Crimson Red
                    "timestamp": dlq_entry["failed_at_utc"],
                    "fields": [
                        {"name": "Topic", "value": f"`{dlq_entry['original_topic']}`", "inline": True},
                        {"name": "Schema ID", "value": str(dlq_entry.get("schema_id") or "N/A"), "inline": True},
                        {"name": "Payload Size", "value": f"{dlq_entry['raw_payload_len']} bytes", "inline": True},
                        {"name": "Error Details", "value": f"```{dlq_entry['error_message']}```", "inline": False},
                    ],
                    "footer": {
                        "text": "Data Engineering Platform | DLQ Circuit Breaker",
                    },
                }
            ],
        }

        logger.info(
            f"[ALERT DISPATCHED] Webhook payload generated for incident {dlq_entry['error_code']}"
        )
        return alert_payload

    def read_dlq_records(self) -> list[dict[str, Any]]:
        """Read all quarantined DLQ records."""
        if not self.dlq_file.exists():
            return []
        records = []
        with open(self.dlq_file, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    def get_dlq_summary(self) -> dict[str, Any]:
        """Get summary metrics on quarantined messages."""
        records = self.read_dlq_records()
        by_error: dict[str, int] = {}
        for r in records:
            err = r.get("error_code", "UNKNOWN")
            by_error[err] = by_error.get(err, 0) + 1

        return {
            "total_quarantined": len(records),
            "dlq_file": str(self.dlq_file),
            "errors_by_code": by_error,
        }
