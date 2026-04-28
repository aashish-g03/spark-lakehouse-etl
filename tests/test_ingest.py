"""Tests for the ingest (bronze) layer."""

import json
import os
import tempfile

from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import functions as F

from pipeline.ingest import ingest


def _write_test_json(records: list[dict]) -> str:
    tmp = tempfile.mkdtemp()
    path = os.path.join(tmp, "test.json")
    with open(path, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")
    return tmp


class TestIngestValidation:
    """Validate that the ingest stage correctly separates clean from bad records."""

    GOOD_RECORD = {
        "transaction_id": "txn-001",
        "customer_id": "CUST-001",
        "amount": 100.0,
        "currency": "USD",
        "transaction_type": "deposit",
        "merchant_category": "bank_transfer",
        "event_date": "2026-04-01",
        "event_timestamp": "2026-04-01T10:00:00",
    }

    def test_clean_record_passes(self, spark):
        path = _write_test_json([self.GOOD_RECORD])
        clean, quarantine = ingest(spark, path)
        assert clean.count() == 1
        assert quarantine.count() == 0

    def test_null_amount_quarantined(self, spark):
        bad = {**self.GOOD_RECORD, "transaction_id": "txn-bad-1", "amount": None}
        path = _write_test_json([self.GOOD_RECORD, bad])
        clean, quarantine = ingest(spark, path)
        assert clean.count() == 1
        assert quarantine.count() == 1

    def test_negative_amount_quarantined(self, spark):
        bad = {**self.GOOD_RECORD, "transaction_id": "txn-bad-2", "amount": -50.0}
        path = _write_test_json([bad])
        clean, quarantine = ingest(spark, path)
        assert clean.count() == 0
        assert quarantine.count() == 1

    def test_bad_currency_quarantined(self, spark):
        bad = {**self.GOOD_RECORD, "transaction_id": "txn-bad-3", "currency": "ZZZZZ"}
        path = _write_test_json([bad])
        clean, quarantine = ingest(spark, path)
        assert clean.count() == 0
        assert quarantine.count() == 1

    def test_null_customer_quarantined(self, spark):
        bad = {**self.GOOD_RECORD, "transaction_id": "txn-bad-4", "customer_id": None}
        path = _write_test_json([bad])
        clean, quarantine = ingest(spark, path)
        assert clean.count() == 0
        assert quarantine.count() == 1

    def test_future_date_quarantined(self, spark):
        bad = {**self.GOOD_RECORD, "transaction_id": "txn-bad-5", "event_date": "2030-01-01"}
        path = _write_test_json([bad])
        clean, quarantine = ingest(spark, path)
        assert clean.count() == 0
        assert quarantine.count() == 1

    def test_bad_transaction_type_quarantined(self, spark):
        bad = {**self.GOOD_RECORD, "transaction_id": "txn-bad-6",
               "transaction_type": "refund"}
        path = _write_test_json([bad])
        clean, quarantine = ingest(spark, path)
        assert clean.count() == 0
        assert quarantine.count() == 1

    def test_schema_evolution_channel_field(self, spark):
        """Records with the extra 'channel' field should still pass validation."""
        record_with_channel = {**self.GOOD_RECORD, "channel": "mobile"}
        path = _write_test_json([record_with_channel])
        clean, quarantine = ingest(spark, path)
        assert clean.count() == 1
        assert "channel" in clean.columns
