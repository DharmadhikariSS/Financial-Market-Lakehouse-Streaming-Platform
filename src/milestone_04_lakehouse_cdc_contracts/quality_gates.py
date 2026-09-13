"""Data Quality Contract & Quarantine Gate Engine.

Implements Option B (Quarantine / Dead-Letter Queue) pattern for streaming CDC mutations:
- Validates primary key presence, numeric sanity bounds, and anomaly price spikes.
- Routes clean records to the Apache Iceberg ACID lakehouse.
- Quarantines malformed/poison-pill payloads into an audit log with diagnostic error headers.
- Emits structured webhook notifications.
"""

import json
from datetime import UTC, datetime
from pathlib import Path

from src.common.config import BASE_DIR
from src.common.logger import get_logger
from src.milestone_04_lakehouse_cdc_contracts.cdc_consumer import CdcMutationEvent

logger = get_logger("milestone_04.quality_gates")

DEFAULT_QUARANTINE_PATH = (
    BASE_DIR / "data" / "lakehouse" / "quarantine" / "corrupt_cdc_events.jsonl"
)

# Price guardrails to detect fat-finger spikes / anomaly drift
MAX_ALLOWABLE_PRICES = {
    "BTCUSDT": 250_000.0,
    "ETHUSDT": 25_000.0,
    "SOLUSDT": 2_500.0,
}


class QualityContractGate:
    """Enforces data quality contracts on streaming CDC events."""

    def __init__(self, quarantine_file: Path | None = None) -> None:
        self.quarantine_path = quarantine_file or DEFAULT_QUARANTINE_PATH
        self.quarantine_path.parent.mkdir(parents=True, exist_ok=True)

    def evaluate_mutation(self, event: CdcMutationEvent) -> list[str]:
        """Evaluates a single mutation event against contract rules. Returns list of violation messages."""
        violations: list[str] = []
        payload = event.payload

        # Rule 1: Primary Key Not Null & Positive
        trade_id = payload.get("trade_id")
        if trade_id is None or not isinstance(trade_id, int) or trade_id <= 0:
            violations.append("PRIMARY_KEY_VIOLATION: trade_id is null or <= 0")

        # Rule 2: Symbol valid
        symbol = str(payload.get("symbol", "")).strip().upper()
        if not symbol or len(symbol) > 20:
            violations.append("SCHEMA_VIOLATION: symbol is empty or exceeds 20 characters")

        # For delete operations, we only require key validity
        if event.op_type == "delete":
            return violations

        # Rule 3: Positive price and quantity
        price = payload.get("price")
        if price is None or not isinstance(price, int | float) or price <= 0:
            violations.append(f"RANGE_VIOLATION: price must be positive (got {price})")

        quantity = payload.get("quantity")
        if quantity is None or not isinstance(quantity, int | float) or quantity <= 0:
            violations.append(f"RANGE_VIOLATION: quantity must be positive (got {quantity})")

        # Rule 4: Anomaly Drift Detection (Fat-finger price spike gate)
        if isinstance(price, int | float) and symbol in MAX_ALLOWABLE_PRICES:
            max_limit = MAX_ALLOWABLE_PRICES[symbol]
            if price > max_limit:
                violations.append(
                    f"ANOMALY_DRIFT_VIOLATION: price {price} exceeds plausibility threshold of {max_limit}"
                )


        return violations

    def filter_and_quarantine(
        self,
        events: list[CdcMutationEvent],
    ) -> tuple[list[CdcMutationEvent], int]:
        """Filters valid mutations and dispatches quarantined poison pills."""
        valid_events: list[CdcMutationEvent] = []
        quarantined_count = 0

        for ev in events:
            violations = self.evaluate_mutation(ev)

            if not violations:
                valid_events.append(ev)
            else:
                quarantined_count += 1
                self._quarantine_event(ev, violations)

        logger.info(
            f"Quality Gate Evaluation: {len(valid_events)} approved, {quarantined_count} quarantined."
        )
        return valid_events, quarantined_count

    def _quarantine_event(self, event: CdcMutationEvent, violations: list[str]) -> None:
        """Appends quarantined payload to the quarantine audit log with error diagnostics."""
        diagnostic_record = {
            "quarantined_at_utc": datetime.now(UTC).isoformat(),
            "op_type": event.op_type,
            "symbol": event.symbol,
            "trade_id": event.trade_id,
            "violations": violations,
            "raw_payload": event.payload,
            "lsn": event.lsn,
        }

        with open(self.quarantine_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(diagnostic_record) + "\n")

        logger.warning(
            f"[QUARANTINE ALERT] Rejected event {event.symbol}#{event.trade_id}: {violations}"
        )
