"""Tests for the reconciliation module."""

import json
import os
import tempfile

from pyspark.sql import Row

from pipeline.reconcile import DayReconciliation, ReconciliationReport, load_manifest


class TestLoadManifest:

    def test_reads_manifest(self):
        data = [{"date": "2026-04-01", "expected_row_count": 100,
                 "expected_sum_amount": 5000.0, "bad_records_injected": 2,
                 "duplicates_injected": 1}]
        path = os.path.join(tempfile.mkdtemp(), "manifest.json")
        with open(path, "w") as f:
            json.dump(data, f)

        result = load_manifest(path)
        assert len(result) == 1
        assert result[0]["expected_row_count"] == 100


class TestReconciliationReport:

    def test_report_structure(self):
        detail = DayReconciliation(
            date="2026-04-01", source_rows=100, sink_rows=97,
            row_delta=3, source_sum=5000.0, sink_sum=4850.0,
            sum_delta=150.0, status="OK", notes="expected loss ~3",
        )
        report = ReconciliationReport(
            total_source_rows=100, total_sink_rows=97,
            total_quarantined=2, total_duplicates_removed=1,
            days_checked=1, days_ok=1, days_mismatch=0,
            details=[detail],
        )
        assert report.days_ok == 1
        assert report.total_source_rows - report.total_sink_rows == 3
