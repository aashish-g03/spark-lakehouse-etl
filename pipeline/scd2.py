"""
SCD Type 2 implementation for dim_customer.

When a customer's tier or address changes, we:
  1. Close the existing row (set effective_to = change_date - 1, is_current = false)
  2. Insert a new row with the updated attributes (effective_from = change_date, is_current = true)

This preserves the full history of every customer state, which is critical for
temporal queries like "what tier was customer X on date Y?"

Uses temp views for the UPDATE to keep everything distributed — no .collect()
to the driver, no SQL string interpolation.
"""

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


SCD2_TABLE = "local.lakehouse.dim_customer"

TRACKED_COLS = ["tier", "address", "city"]


def build_initial_dimension(customers: DataFrame, as_of: date) -> DataFrame:
    """First load: every customer gets one active row."""
    return (
        customers
        .withColumn("customer_sk", F.expr("uuid()"))
        .withColumn("effective_from", F.lit(as_of))
        .withColumn("effective_to", F.lit(date(9999, 12, 31)))
        .withColumn("is_current", F.lit(True))
    )


def create_dim_table(spark: SparkSession, initial: DataFrame):
    initial.writeTo(SCD2_TABLE).createOrReplace()


def detect_changes(current_dim: DataFrame, incoming: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Compare incoming customer records against the current dimension snapshot.
    Returns (changed, new_customers).

    Uses null-safe comparison so that NULL -> 'value' transitions are detected.
    """
    current_active = current_dim.filter(F.col("is_current")).select(
        F.col("customer_id"),
        *[F.col(c).alias(f"old_{c}") for c in TRACKED_COLS],
    )

    joined = incoming.join(current_active, on="customer_id", how="left")

    # Customers not in the current dimension (new arrivals after initial load)
    is_new = F.col(f"old_{TRACKED_COLS[0]}").isNull()
    new_customers = joined.filter(is_new).select(incoming.columns)

    # Existing customers where at least one tracked column changed
    has_match = ~is_new
    change_condition = F.lit(False)
    for c in TRACKED_COLS:
        change_condition = change_condition | ~F.col(c).eqNullSafe(F.col(f"old_{c}"))

    changed = joined.filter(has_match & change_condition).select(incoming.columns)
    return changed, new_customers


def apply_scd2(spark: SparkSession, changes: DataFrame, new_customers: DataFrame,
               change_date: date):
    """
    Merge changed and new records into the dimension table.

    For changed customers: expire the current row, insert a new version.
    For new customers: insert directly as a new current row.

    Uses a temp view for the UPDATE subquery — keeps everything distributed,
    no driver-side collect or SQL string construction.
    """
    expire_date = date.fromordinal(change_date.toordinal() - 1)

    # Expire current rows for changed customers
    if not changes.isEmpty():
        changes.select("customer_id").createOrReplaceTempView("_scd2_changes")

        spark.sql(f"""
            UPDATE {SCD2_TABLE}
            SET effective_to = DATE '{expire_date.isoformat()}',
                is_current = false
            WHERE customer_id IN (SELECT customer_id FROM _scd2_changes)
              AND is_current = true
        """)

        spark.catalog.dropTempView("_scd2_changes")

    # Build new version rows for changed customers
    rows_to_insert = changes.withColumn("_type", F.lit("changed"))

    # Also include genuinely new customers
    if not new_customers.isEmpty():
        rows_to_insert = rows_to_insert.unionByName(
            new_customers.withColumn("_type", F.lit("new"))
        )

    if rows_to_insert.isEmpty():
        return

    new_rows = (
        rows_to_insert
        .drop("_type")
        .withColumn("customer_sk", F.expr("uuid()"))
        .withColumn("effective_from", F.lit(change_date))
        .withColumn("effective_to", F.lit(date(9999, 12, 31)))
        .withColumn("is_current", F.lit(True))
    )

    new_rows.writeTo(SCD2_TABLE).append()


def process_scd2(
    spark: SparkSession,
    customers_path: str,
    changes_path: str,
    initial_date: date,
    change_date: date,
):
    """Full SCD2 flow: initial load, then detect and apply changes."""
    from pipeline.schemas import CUSTOMER_SCHEMA

    customers = spark.read.schema(CUSTOMER_SCHEMA).json(customers_path)
    initial = build_initial_dimension(customers, as_of=initial_date)
    create_dim_table(spark, initial)

    changes_df = spark.read.schema(CUSTOMER_SCHEMA).json(changes_path)
    changed, new_customers = detect_changes(
        spark.read.table(SCD2_TABLE),
        changes_df,
    )
    apply_scd2(spark, changed, new_customers, change_date)
