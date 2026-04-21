"""
Reconciliation: compare source manifest against what landed in the Iceberg table.

The manifest (produced by data_generator.py) records expected row counts and sum(amount)
per day. After the pipeline writes to Iceberg, we recompute the same metrics from the
output and flag any discrepancies.

Expected discrepancies are documented:
  - Row count will be lower than source because quarantined records are excluded
  - Sum will differ because quarantined records (negative amounts, nulls) are excluded
  - Duplicates removed during transform also reduce row count
"""

import json
from dataclasses import asdict, dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass
class DayReconciliation:
    date: str
    source_rows: int
    sink_rows: int
    row_delta: int
    source_sum: float
    sink_sum: float
    sum_delta: float
    status: str  # "OK" or "MISMATCH"
    notes: str


@dataclass
class ReconciliationReport:
    total_source_rows: int
    total_sink_rows: int
    total_quarantined: int
    total_duplicates_removed: int
    days_checked: int
    days_ok: int
    days_mismatch: int
    details: list[DayReconciliation]


def load_manifest(path: str) -> list[dict]:
    with open(path) as f:
        return json.load(f)


def compute_sink_metrics(fact_table: DataFrame) -> DataFrame:
    """Aggregate row count and sum(amount) per event_date from the Iceberg table."""
    return (
        fact_table
        .groupBy("event_date")
        .agg(
            F.count("*").alias("sink_rows"),
            F.round(F.sum("amount"), 2).alias("sink_sum"),
        )
        .orderBy("event_date")
    )


def reconcile(
    spark: SparkSession,
    fact_table_name: str,
    manifest_path: str,
    quarantine_count: int,
    duplicates_removed: int,
) -> ReconciliationReport:
    manifest = load_manifest(manifest_path)
    fact_df = spark.read.table(fact_table_name)
    sink_metrics = compute_sink_metrics(fact_df).collect()

    sink_by_date = {
        row.event_date.isoformat(): {"rows": row.sink_rows, "sum": float(row.sink_sum)}
        for row in sink_metrics
    }

    details = []
    days_ok = 0

    for entry in manifest:
        d = entry["date"]
        source_rows = entry["expected_row_count"]
        source_sum = entry["expected_sum_amount"]
        sink = sink_by_date.get(d, {"rows": 0, "sum": 0.0})

        row_delta = source_rows - sink["rows"]
        sum_delta = round(source_sum - sink["sum"], 2)

        # Row delta should roughly equal bad + dupes for that day
        expected_row_loss = entry["bad_records_injected"] + entry["duplicates_injected"]
        is_ok = abs(row_delta - expected_row_loss) <= 1  # tolerance for edge cases

        day_result = DayReconciliation(
            date=d,
            source_rows=source_rows,
            sink_rows=sink["rows"],
            row_delta=row_delta,
            source_sum=source_sum,
            sink_sum=sink["sum"],
            sum_delta=sum_delta,
            status="OK" if is_ok else "MISMATCH",
            notes=f"expected loss ~{expected_row_loss} (bad+dupes)" if is_ok else "investigate",
        )
        details.append(day_result)
        if is_ok:
            days_ok += 1

    return ReconciliationReport(
        total_source_rows=sum(e["expected_row_count"] for e in manifest),
        total_sink_rows=sum(d.sink_rows for d in details),
        total_quarantined=quarantine_count,
        total_duplicates_removed=duplicates_removed,
        days_checked=len(details),
        days_ok=days_ok,
        days_mismatch=len(details) - days_ok,
        details=details,
    )


def save_report(report: ReconciliationReport, path: str):
    with open(path, "w") as f:
        json.dump(asdict(report), f, indent=2, default=str)
