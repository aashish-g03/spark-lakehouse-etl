"""Tests for the SCD Type 2 dimension logic."""

from datetime import date

from pyspark.sql import Row, functions as F

from pipeline.scd2 import (
    SCD2_TABLE,
    build_initial_dimension,
    create_dim_table,
    detect_changes,
    apply_scd2,
)


def _make_customer_df(spark, overrides=None):
    base = {
        "customer_id": "C1", "name": "Alice", "email": "a@b.com",
        "address": "123 Main St", "city": "NYC", "country": "US", "tier": "silver",
    }
    if overrides:
        base.update(overrides)
    return spark.createDataFrame([Row(**base)])


class TestInitialLoad:

    def test_adds_scd2_columns(self, spark):
        customers = _make_customer_df(spark)
        result = build_initial_dimension(customers, as_of=date(2026, 4, 1))
        row = result.first()
        assert row.is_current is True
        assert row.effective_from == date(2026, 4, 1)
        assert row.effective_to == date(9999, 12, 31)
        assert row.customer_sk is not None


class TestDetectChanges:

    def test_detects_tier_change(self, spark):
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.lakehouse")
        current = _make_customer_df(spark, {"tier": "silver"})
        dim = build_initial_dimension(current, as_of=date(2026, 4, 1))
        create_dim_table(spark, dim)

        incoming = _make_customer_df(spark, {"tier": "gold"})
        changes = detect_changes(spark.read.table(SCD2_TABLE), incoming)
        assert changes.count() == 1
        assert changes.first().tier == "gold"

    def test_no_change_returns_empty(self, spark):
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.lakehouse")
        current = _make_customer_df(spark)
        dim = build_initial_dimension(current, as_of=date(2026, 4, 1))
        create_dim_table(spark, dim)

        same = _make_customer_df(spark)
        changes = detect_changes(spark.read.table(SCD2_TABLE), same)
        assert changes.count() == 0


class TestApplySCD2:

    def test_expire_and_insert(self, spark):
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.lakehouse")
        current = _make_customer_df(spark, {"tier": "silver"})
        dim = build_initial_dimension(current, as_of=date(2026, 4, 1))
        create_dim_table(spark, dim)

        changed = _make_customer_df(spark, {"tier": "gold"})
        apply_scd2(spark, changed, change_date=date(2026, 4, 16))

        result = spark.read.table(SCD2_TABLE)
        assert result.count() == 2  # one expired + one new

        expired = result.filter("is_current = false").first()
        assert expired.tier == "silver"
        assert expired.effective_to == date(2026, 4, 15)

        active = result.filter("is_current = true").first()
        assert active.tier == "gold"
        assert active.effective_from == date(2026, 4, 16)
