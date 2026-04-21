"""
Generates synthetic financial transaction data for the ETL pipeline.

Produces:
  - data/raw/customers.json         (reference dimension, ~10K rows)
  - data/raw/transactions/day=YYYY-MM-DD/*.json  (30 days of facts)
  - data/raw/manifest.json          (row counts + sums per day for reconciliation)
  - data/raw/customer_changes.json  (tier/address changes for SCD2 testing)

Run: python data_generator.py [--num-customers 10000] [--days 30] [--seed 42]
"""

import argparse
import json
import os
import random
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta

from faker import Faker

BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "raw")

TRANSACTION_TYPES = ["deposit", "withdrawal", "transfer", "payment"]
MERCHANT_CATEGORIES = [
    "retail", "grocery", "restaurant", "travel", "entertainment",
    "utilities", "healthcare", "education", "bank_transfer", "atm",
]
CURRENCIES = ["USD", "EUR", "GBP", "INR"]
TIERS = ["bronze", "silver", "gold", "platinum"]

# Approx transactions per day — scaled to hit ~10M over 30 days
DEFAULT_TXN_PER_DAY = 333_000


def generate_customers(fake: Faker, n: int) -> list[dict]:
    customers = []
    for i in range(n):
        cid = f"CUST-{i + 1:06d}"
        customers.append({
            "customer_id": cid,
            "name": fake.name(),
            "email": fake.email(),
            "address": fake.street_address(),
            "city": fake.city(),
            "country": fake.country_code(),
            "tier": random.choice(TIERS),
        })
    return customers


def generate_customer_changes(customers: list[dict], fake: Faker,
                              change_pct: float = 0.05) -> list[dict]:
    """~5% of customers get a tier or address change midway through the period."""
    changes = []
    sample_size = max(1, int(len(customers) * change_pct))
    changed = random.sample(customers, sample_size)

    for cust in changed:
        new_tier = random.choice([t for t in TIERS if t != cust["tier"]])
        changes.append({
            "customer_id": cust["customer_id"],
            "name": cust["name"],
            "email": cust["email"],
            "address": fake.street_address(),
            "city": fake.city(),
            "country": cust["country"],
            "tier": new_tier,
        })
    return changes


def generate_day_transactions(
    fake: Faker,
    customer_ids: list[str],
    event_date: date,
    count: int,
    include_channel: bool = False,
) -> tuple[list[dict], dict]:
    """
    Returns (records, manifest_entry).
    After day 15 we add a 'channel' field to simulate schema evolution.
    """
    records = []
    daily_sum = 0.0
    bad_count = 0

    for i in range(count):
        is_bad = random.random() < 0.005  # ~0.5% bad records

        if is_bad:
            record = _make_bad_record(fake, customer_ids, event_date)
            bad_count += 1
        else:
            amount = round(random.uniform(1.0, 50_000.0), 2)
            record = {
                "transaction_id": str(uuid.uuid4()),
                "customer_id": random.choice(customer_ids),
                "amount": amount,
                "currency": random.choice(CURRENCIES),
                "transaction_type": random.choice(TRANSACTION_TYPES),
                "merchant_category": random.choice(MERCHANT_CATEGORIES),
                "event_date": event_date.isoformat(),
                "event_timestamp": _random_timestamp(fake, event_date),
            }
            daily_sum += amount

        if include_channel:
            record["channel"] = random.choice(["mobile", "web", "branch", "atm"])

        records.append(record)

    # Inject a few exact duplicates
    n_dupes = max(1, count // 2000)
    for _ in range(n_dupes):
        records.append(random.choice(records[:count]))

    manifest_entry = {
        "date": event_date.isoformat(),
        "expected_row_count": count,  # excludes dupes and bad records
        "expected_sum_amount": round(daily_sum, 2),
        "bad_records_injected": bad_count,
        "duplicates_injected": n_dupes,
    }
    return records, manifest_entry


def _make_bad_record(fake: Faker, customer_ids: list[str], event_date: date) -> dict:
    """Produces a record with one or more quality issues."""
    defect = random.choice(["null_amount", "negative", "future_date", "bad_currency", "missing_customer"])

    record = {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": random.choice(customer_ids),
        "amount": round(random.uniform(1.0, 50_000.0), 2),
        "currency": random.choice(CURRENCIES),
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "event_date": event_date.isoformat(),
        "event_timestamp": _random_timestamp(fake, event_date),
    }

    if defect == "null_amount":
        record["amount"] = None
    elif defect == "negative":
        record["amount"] = -abs(record["amount"])
    elif defect == "future_date":
        future = event_date + timedelta(days=random.randint(365, 730))
        record["event_date"] = future.isoformat()
        record["event_timestamp"] = _random_timestamp(fake, future)
    elif defect == "bad_currency":
        record["currency"] = "ZZZZZ"
    elif defect == "missing_customer":
        record["customer_id"] = None

    return record


def _random_timestamp(fake: Faker, d: date) -> str:
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return datetime(d.year, d.month, d.day, hour, minute, second).isoformat()


def write_jsonl(records: list[dict], path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic financial data")
    parser.add_argument("--num-customers", type=int, default=10_000)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--txn-per-day", type=int, default=DEFAULT_TXN_PER_DAY)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--small", action="store_true",
                        help="Generate a small dataset for testing (~1K txn/day)")
    args = parser.parse_args()

    if args.small:
        args.num_customers = 500
        args.txn_per_day = 1_000
        args.days = 10

    random.seed(args.seed)
    fake = Faker()
    Faker.seed(args.seed)

    print(f"Generating {args.num_customers} customers...")
    customers = generate_customers(fake, args.num_customers)
    write_jsonl(customers, os.path.join(BASE_DIR, "customers.json"))

    print(f"Generating customer changes (~5% tier/address mutations)...")
    changes = generate_customer_changes(customers, fake)
    write_jsonl(changes, os.path.join(BASE_DIR, "customer_changes.json"))

    customer_ids = [c["customer_id"] for c in customers]
    start_date = date(2026, 4, 1)
    manifest = []

    print(f"Generating {args.days} days of transactions ({args.txn_per_day}/day)...")
    for day_offset in range(args.days):
        current_date = start_date + timedelta(days=day_offset)
        include_channel = day_offset >= 15  # schema evolution after day 15

        records, manifest_entry = generate_day_transactions(
            fake, customer_ids, current_date, args.txn_per_day,
            include_channel=include_channel,
        )
        manifest.append(manifest_entry)

        day_dir = os.path.join(BASE_DIR, "transactions", f"day={current_date.isoformat()}")
        write_jsonl(records, os.path.join(day_dir, "part-0000.json"))

        total_with_dupes = len(records)
        tag = " [+channel]" if include_channel else ""
        print(f"  {current_date}: {total_with_dupes} records{tag}")

    manifest_path = os.path.join(BASE_DIR, "manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    total_txn = sum(e["expected_row_count"] for e in manifest)
    total_bad = sum(e["bad_records_injected"] for e in manifest)
    total_dupes = sum(e["duplicates_injected"] for e in manifest)
    print(f"\nDone. {total_txn:,} clean transactions + {total_bad:,} bad + {total_dupes:,} dupes")
    print(f"Output: {BASE_DIR}")


if __name__ == "__main__":
    main()
