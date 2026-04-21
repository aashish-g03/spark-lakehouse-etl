"""
Pipeline entrypoint. Runs the full ETL: ingest -> transform -> write -> SCD2 -> reconcile.

Usage:
    python data_generator.py --small   # generate test data first
    python main.py                     # run pipeline
    python main.py --data-dir /path/to/data/raw
"""

import argparse
import os
import shutil
import time
from datetime import date

from pipeline.spark_session import WAREHOUSE_PATH, get_spark
from pipeline.ingest import ingest
from pipeline.transform import transform
from pipeline.scd2 import process_scd2
from pipeline.reconcile import reconcile, save_report

FACT_TABLE = "local.lakehouse.fact_transactions"

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "output")


def run_pipeline(data_dir: str, clean_warehouse: bool = True):
    txn_path = os.path.join(data_dir, "transactions")
    customers_path = os.path.join(data_dir, "customers.json")
    changes_path = os.path.join(data_dir, "customer_changes.json")
    manifest_path = os.path.join(data_dir, "manifest.json")

    for p in [txn_path, customers_path, manifest_path]:
        if not os.path.exists(p):
            raise FileNotFoundError(
                f"Missing {p}. Run 'python data_generator.py --small' first."
            )

    if clean_warehouse and os.path.exists(WAREHOUSE_PATH):
        shutil.rmtree(WAREHOUSE_PATH)

    spark = get_spark()
    t0 = time.time()

    try:
        # --- Ingest (raw -> bronze) ---
        print("=" * 60)
        print("STAGE 1: Ingest")
        print("=" * 60)
        clean, quarantine = ingest(spark, txn_path)

        quarantine_count = quarantine.count()
        clean_count = clean.count()
        print(f"  Clean records:      {clean_count:,}")
        print(f"  Quarantined:        {quarantine_count:,}")

        # Persist quarantine for auditing
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        quarantine_path = os.path.join(OUTPUT_DIR, "quarantine")
        quarantine.write.mode("overwrite").json(quarantine_path)

        # --- Transform (bronze -> silver) ---
        print("\n" + "=" * 60)
        print("STAGE 2: Transform")
        print("=" * 60)
        pre_dedup = clean_count
        silver = transform(spark, clean, customers_path)
        post_dedup = silver.count()
        dupes_removed = pre_dedup - post_dedup
        print(f"  Duplicates removed: {dupes_removed:,}")
        print(f"  Enriched records:   {post_dedup:,}")

        # --- Write fact table to Iceberg ---
        print("\n" + "=" * 60)
        print("STAGE 3: Write to Iceberg")
        print("=" * 60)

        # Namespace needs to exist before writing
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.lakehouse")

        silver.writeTo(FACT_TABLE).using("iceberg").partitionedBy(
            "event_date"
        ).createOrReplace()

        written = spark.read.table(FACT_TABLE).count()
        print(f"  Wrote {written:,} rows to {FACT_TABLE}")

        # --- SCD Type 2 ---
        print("\n" + "=" * 60)
        print("STAGE 4: SCD Type 2 (dim_customer)")
        print("=" * 60)
        process_scd2(
            spark,
            customers_path=customers_path,
            changes_path=changes_path,
            initial_date=date(2026, 4, 1),
            change_date=date(2026, 4, 16),  # changes applied mid-period
        )

        dim = spark.read.table("local.lakehouse.dim_customer")
        total_rows = dim.count()
        current_rows = dim.filter("is_current = true").count()
        historical_rows = total_rows - current_rows
        print(f"  Total dimension rows: {total_rows:,}")
        print(f"  Current:              {current_rows:,}")
        print(f"  Historical:           {historical_rows:,}")

        # --- Reconciliation ---
        print("\n" + "=" * 60)
        print("STAGE 5: Reconciliation")
        print("=" * 60)
        report = reconcile(
            spark,
            fact_table_name=FACT_TABLE,
            manifest_path=manifest_path,
            quarantine_count=quarantine_count,
            duplicates_removed=dupes_removed,
        )

        report_path = os.path.join(OUTPUT_DIR, "reconciliation_report.json")
        save_report(report, report_path)

        print(f"  Source rows:    {report.total_source_rows:,}")
        print(f"  Sink rows:      {report.total_sink_rows:,}")
        print(f"  Quarantined:    {report.total_quarantined:,}")
        print(f"  Dupes removed:  {report.total_duplicates_removed:,}")
        print(f"  Days OK:        {report.days_ok}/{report.days_checked}")
        if report.days_mismatch > 0:
            print(f"  MISMATCHES:     {report.days_mismatch} days need investigation")
        print(f"  Report saved:   {report_path}")

        elapsed = time.time() - t0
        print(f"\nPipeline complete in {elapsed:.1f}s")

    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Run the lakehouse ETL pipeline")
    parser.add_argument("--data-dir", default=DEFAULT_DATA_DIR,
                        help="Path to raw data directory")
    parser.add_argument("--keep-warehouse", action="store_true",
                        help="Don't wipe warehouse before running")
    args = parser.parse_args()

    run_pipeline(args.data_dir, clean_warehouse=not args.keep_warehouse)


if __name__ == "__main__":
    main()
