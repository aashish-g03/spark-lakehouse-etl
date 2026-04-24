"""Tests for the transform (silver) layer."""

from pyspark.sql import Row, functions as F

from pipeline.transform import deduplicate, enrich, add_derived_columns


class TestDeduplicate:

    def test_removes_exact_dupes(self, spark):
        data = [
            Row(transaction_id="txn-1", amount=100.0),
            Row(transaction_id="txn-1", amount=100.0),
            Row(transaction_id="txn-2", amount=200.0),
        ]
        df = spark.createDataFrame(data)
        result = deduplicate(df)
        assert result.count() == 2

    def test_no_dupes_unchanged(self, spark):
        data = [
            Row(transaction_id="txn-1", amount=100.0),
            Row(transaction_id="txn-2", amount=200.0),
        ]
        df = spark.createDataFrame(data)
        result = deduplicate(df)
        assert result.count() == 2


class TestEnrich:

    def test_join_adds_customer_columns(self, spark):
        txns = spark.createDataFrame([
            Row(customer_id="C1", amount=50.0),
        ])
        custs = spark.createDataFrame([
            Row(customer_id="C1", name="Alice", email="a@b.com",
                address="123 St", city="NY", country="US", tier="gold"),
        ])
        result = enrich(txns, custs)
        assert "customer_name" in result.columns
        assert "customer_tier" in result.columns
        row = result.first()
        assert row.customer_name == "Alice"
        assert row.customer_tier == "gold"

    def test_missing_customer_left_join(self, spark):
        """Transactions with unknown customer_id should still appear, with nulls."""
        txns = spark.createDataFrame([
            Row(customer_id="UNKNOWN", amount=50.0),
        ])
        custs = spark.createDataFrame([
            Row(customer_id="C1", name="Alice", email="a@b.com",
                address="123 St", city="NY", country="US", tier="gold"),
        ])
        result = enrich(txns, custs)
        assert result.count() == 1
        assert result.first().customer_name is None


class TestDerivedColumns:

    def test_usd_passthrough(self, spark):
        df = spark.createDataFrame([Row(amount=100.0, currency="USD")])
        result = add_derived_columns(df)
        assert result.first().amount_usd == 100.0

    def test_eur_conversion(self, spark):
        df = spark.createDataFrame([Row(amount=100.0, currency="EUR")])
        result = add_derived_columns(df)
        assert result.first().amount_usd == 108.0

    def test_inr_conversion(self, spark):
        df = spark.createDataFrame([Row(amount=10000.0, currency="INR")])
        result = add_derived_columns(df)
        assert result.first().amount_usd == 120.0
