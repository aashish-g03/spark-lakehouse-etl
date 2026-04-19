"""
Smoke test: generate a few rows of JSON, read into Spark, write to Iceberg,
read back and print. Validates the full stack works locally before building
the real pipeline.

Usage: python smoke_test.py
"""

import json
import os
import shutil
import tempfile
from datetime import date, datetime
from decimal import Decimal

from pipeline.spark_session import WAREHOUSE_PATH, get_spark

SAMPLE_TRANSACTIONS = [
    {
        "customer_id": "CUST-001",
        "amount": 1250.00,
        "currency": "USD",
        "transaction_type": "deposit",
        "merchant_category": "bank_transfer",
        "event_date": "2026-05-01",
        "event_timestamp": "2026-05-01T09:30:00",
    },
    {
        "customer_id": "CUST-002",
        "amount": 89.99,
        "currency": "USD",
        "transaction_type": "payment",
        "merchant_category": "retail",
        "event_date": "2026-05-01",
        "event_timestamp": "2026-05-01T14:22:11",
    },
    {
        "customer_id": "CUST-001",
        "amount": -500.00,
        "currency": "USD",
        "transaction_type": "withdrawal",
        "merchant_category": "atm",
        "event_date": "2026-05-02",
        "event_timestamp": "2026-05-02T11:05:30",
    },
]


def main():
    # Write sample JSON to a temp file
    tmp_dir = tempfile.mkdtemp(prefix="spark_smoke_")
    json_path = os.path.join(tmp_dir, "transactions.json")

    with open(json_path, "w") as f:
        for txn in SAMPLE_TRANSACTIONS:
            f.write(json.dumps(txn) + "\n")

    print(f"Wrote {len(SAMPLE_TRANSACTIONS)} sample records to {json_path}")

    # Clean previous warehouse state
    if os.path.exists(WAREHOUSE_PATH):
        shutil.rmtree(WAREHOUSE_PATH)

    spark = get_spark("smoke-test")

    try:
        # Read JSON
        df = spark.read.json(json_path)
        print(f"\n--- Raw DataFrame ({df.count()} rows) ---")
        df.printSchema()
        df.show(truncate=False)

        # Create Iceberg table and write
        df.writeTo("local.smoke.transactions").createOrReplace()
        print("Wrote to Iceberg table: local.smoke.transactions")

        # Read back from Iceberg
        iceberg_df = spark.read.table("local.smoke.transactions")
        print(f"\n--- Read back from Iceberg ({iceberg_df.count()} rows) ---")
        iceberg_df.show(truncate=False)

        print("\nSmoke test passed.")
    finally:
        spark.stop()
        shutil.rmtree(tmp_dir)


if __name__ == "__main__":
    main()
