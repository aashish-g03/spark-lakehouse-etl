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
    from pipeline.schemas import CUSTOMER_SCHEMA
    base = {
        "customer_id": "C1", "name": "Alice", "email": "a@b.com",
        "address": "123 Main St", "city": "NYC", "country": "US", "tier": "silver",
    }
    if overrides:
        base.update(overrides)
    return spark.createDataFrame([Row(**base)], schema=CUSTOMER_SCHEMA)


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
        changed, new = detect_changes(spark.read.table(SCD2_TABLE), incoming)
        assert changed.count() == 1
        assert changed.first().tier == "gold"
        assert new.count() == 0

    def test_no_change_returns_empty(self, spark):
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.lakehouse")
        current = _make_customer_df(spark)
        dim = build_initial_dimension(current, as_of=date(2026, 4, 1))
        create_dim_table(spark, dim)

        same = _make_customer_df(spark)
        changed, new = detect_changes(spark.read.table(SCD2_TABLE), same)
        assert changed.count() == 0
        assert new.count() == 0

    def test_detects_null_to_value_change(self, spark):
        """NULL -> 'NYC' should be detected as a change (null-safe comparison)."""
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.lakehouse")
        current = _make_customer_df(spark, {"city": None})
        dim = build_initial_dimension(current, as_of=date(2026, 4, 1))
        create_dim_table(spark, dim)

        incoming = _make_customer_df(spark, {"city": "NYC"})
        changed, new = detect_changes(spark.read.table(SCD2_TABLE), incoming)
        assert changed.count() == 1

    def test_new_customer_detected(self, spark):
        """A customer not in the dimension should appear in the 'new' result."""
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.lakehouse")
        current = _make_customer_df(spark, {"customer_id": "C1"})
        dim = build_initial_dimension(current, as_of=date(2026, 4, 1))
        create_dim_table(spark, dim)

        incoming = _make_customer_df(spark, {"customer_id": "C-NEW", "tier": "gold"})
        changed, new = detect_changes(spark.read.table(SCD2_TABLE), incoming)
        assert changed.count() == 0
        assert new.count() == 1
        assert new.first().customer_id == "C-NEW"


class TestApplySCD2:

    def test_expire_and_insert(self, spark):
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.lakehouse")
        current = _make_customer_df(spark, {"tier": "silver"})
        dim = build_initial_dimension(current, as_of=date(2026, 4, 1))
        create_dim_table(spark, dim)

        changed = _make_customer_df(spark, {"tier": "gold"})
        empty = spark.createDataFrame([], changed.schema)
        apply_scd2(spark, changed, empty, change_date=date(2026, 4, 16))

        result = spark.read.table(SCD2_TABLE)
        assert result.count() == 2

        expired = result.filter("is_current = false").first()
        assert expired.tier == "silver"
        assert expired.effective_to == date(2026, 4, 15)

        active = result.filter("is_current = true").first()
        assert active.tier == "gold"
        assert active.effective_from == date(2026, 4, 16)

    def test_new_customer_inserted(self, spark):
        """New customers get inserted directly with no expiration step."""
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.lakehouse")
        existing = _make_customer_df(spark, {"customer_id": "C1"})
        dim = build_initial_dimension(existing, as_of=date(2026, 4, 1))
        create_dim_table(spark, dim)

        no_changes = spark.createDataFrame([], existing.schema)
        new_cust = _make_customer_df(spark, {"customer_id": "C-NEW", "tier": "gold"})
        apply_scd2(spark, no_changes, new_cust, change_date=date(2026, 4, 16))

        result = spark.read.table(SCD2_TABLE)
        assert result.count() == 2  # C1 original + C-NEW
        assert result.filter("is_current = true").count() == 2

        new_row = result.filter("customer_id = 'C-NEW'").first()
        assert new_row.tier == "gold"
        assert new_row.effective_from == date(2026, 4, 16)

    def test_double_change_creates_three_versions(self, spark):
        """A customer changing twice should produce three rows total."""
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.lakehouse")
        original = _make_customer_df(spark, {"tier": "bronze"})
        dim = build_initial_dimension(original, as_of=date(2026, 4, 1))
        create_dim_table(spark, dim)

        # First change: bronze -> silver
        first_change = _make_customer_df(spark, {"tier": "silver"})
        empty = spark.createDataFrame([], original.schema)
        apply_scd2(spark, first_change, empty, change_date=date(2026, 4, 10))

        # Second change: silver -> gold
        second_change = _make_customer_df(spark, {"tier": "gold"})
        apply_scd2(spark, second_change, empty, change_date=date(2026, 4, 20))

        result = spark.read.table(SCD2_TABLE)
        assert result.count() == 3
        assert result.filter("is_current = true").count() == 1
        assert result.filter("is_current = true").first().tier == "gold"

    def test_empty_changes_is_noop(self, spark):
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.lakehouse")
        current = _make_customer_df(spark)
        dim = build_initial_dimension(current, as_of=date(2026, 4, 1))
        create_dim_table(spark, dim)

        empty = spark.createDataFrame([], current.schema)
        apply_scd2(spark, empty, empty, change_date=date(2026, 4, 16))

        result = spark.read.table(SCD2_TABLE)
        assert result.count() == 1
        assert result.first().is_current is True
