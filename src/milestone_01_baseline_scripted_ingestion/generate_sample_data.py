"""Synthetic Market Trade Data Generator.

Generates:
1. data/unit_test_50.csv: 50 deterministic rows for rapid unit and integration testing.
2. data/audit_trades_100k.csv: 100,000 high-volume records with intentional dirty anomalies
   (duplicates, scientific notation, mixed-case symbols, out-of-order timestamps)
   designed to audit pipeline idempotency and error sanitization.
"""

import csv
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path

# Fix random seed for reproducible audit builds
random.seed(42)

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
BASE_PRICES = {"BTCUSDT": 65000.0, "ETHUSDT": 3500.0, "SOLUSDT": 150.0}


def generate_unit_test_fixture(file_path: Path, count: int = 50) -> None:
    """Generates a small deterministic 50-row fixture."""
    base_time = datetime(2026, 3, 1, 12, 0, 0, tzinfo=UTC)
    rows = []

    for i in range(1, count + 1):
        sym = SYMBOLS[i % len(SYMBOLS)]
        price = round(BASE_PRICES[sym] + (i * 0.5), 2)
        qty = round(0.1 + (i * 0.01), 4)
        quote_qty = round(price * qty, 4)
        t_time = (base_time + timedelta(seconds=i * 2)).isoformat()
        is_buyer = i % 2 == 0

        rows.append(
            {
                "trade_id": 1000000 + i,
                "symbol": sym,
                "price": price,
                "quantity": qty,
                "quote_quantity": quote_qty,
                "trade_timestamp": t_time,
                "is_buyer_maker": is_buyer,
            }
        )

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print(f"[OK] Generated unit test fixture: {file_path} ({count} rows)")


def generate_stress_audit_fixture(file_path: Path, total_records: int = 100_000) -> None:
    """Generates 100,000 stress records with 5% duplicates and dirty edge cases."""
    base_time = datetime(2026, 3, 1, 0, 0, 0, tzinfo=UTC)
    unique_count = int(total_records * 0.95)
    rows = []

    print(f"Generating {total_records} stress records with edge cases...")

    for i in range(1, unique_count + 1):
        sym = random.choice(SYMBOLS)
        base_p = BASE_PRICES[sym]
        volatility = (random.random() - 0.5) * (base_p * 0.05)
        price = round(base_p + volatility, 4)
        qty = round(random.uniform(0.001, 2.5), 6)
        quote_qty = round(price * qty, 4)

        # Random timestamp within a 24h window
        seconds_offset = random.randint(0, 86400)
        t_time = (base_time + timedelta(seconds=seconds_offset)).isoformat()
        is_buyer = random.choice([True, False])

        # Inject intentional dirty anomalies
        sym_to_write = sym
        price_to_write: str | float = price

        # 1. Mixed-case or padded symbol (3% probability)
        if random.random() < 0.03:
            sym_to_write = f" {sym.lower()} " if random.random() < 0.5 else sym.capitalize()

        # 2. Scientific notation formatting (2% probability)
        if random.random() < 0.02:
            price_to_write = f"{price:.4e}"

        rows.append(
            {
                "trade_id": 2000000 + i,
                "symbol": sym_to_write,
                "price": price_to_write,
                "quantity": qty,
                "quote_quantity": quote_qty,
                "trade_timestamp": t_time,
                "is_buyer_maker": str(is_buyer).lower(),
            }
        )

    # Inject 5% Duplicate Primary Keys to stress-test idempotency
    duplicate_count = total_records - unique_count
    print(f"Injecting {duplicate_count} intentional duplicate primary keys...")
    for _ in range(duplicate_count):
        original = random.choice(rows)
        # Duplicate trade_id and symbol, but simulate price update or network replay
        dup_row = dict(original)
        if random.random() < 0.3:
            # Replay with updated price
            orig_price = float(original["price"])
            dup_row["price"] = round(orig_price * 1.001, 4)
            dup_row["quote_quantity"] = round(
                float(dup_row["price"]) * float(dup_row["quantity"]), 4
            )
        rows.append(dup_row)

    # Shuffle to introduce out-of-order event timestamps
    random.shuffle(rows)

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "trade_id",
                "symbol",
                "price",
                "quantity",
                "quote_quantity",
                "trade_timestamp",
                "is_buyer_maker",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"[OK] Generated stress audit fixture: {file_path} ({len(rows)} records)")


def main() -> None:
    generate_unit_test_fixture(DATA_DIR / "unit_test_50.csv", count=50)
    generate_stress_audit_fixture(DATA_DIR / "audit_trades_100k.csv", total_records=100_000)


if __name__ == "__main__":
    main()
