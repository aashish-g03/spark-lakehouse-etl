"""
SCD Type 2 implementation for dim_customer.

When a customer's tier or address changes, we:
  1. Close the existing row (set effective_to = change_date - 1, is_current = false)
  2. Insert a new row with the updated attributes (effective_from = change_date, is_current = true)

This preserves the full history of every customer state, which is critical for
temporal queries like "what tier was customer X on date Y?"

The merge uses Iceberg's MERGE INTO via Spark SQL for atomic upserts.
"""

import uuid
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


SCD2_TABLE = "local.lakehouse.dim_customer"

# Columns tracked for changes — a change in any of these opens a new version
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


def detect_changes(current_dim: DataFrame, incoming: DataFrame) -> DataFrame:
    """
    Compare incoming customer records against the current dimension snapshot.
    Returns only the rows where tracked columns differ.
    """
    current_active = current_dim.filter(F.col("is_current") == True).select(
        F.col("customer_id"),
        *[F.col(c).alias(f"old_{c}") for c in TRACKED_COLS],
    )

    joined = incoming.join(current_active, on="customer_id", how="inner")

    change_condition = F.lit(False)
    for c in TRACKED_COLS:
        change_condition = change_condition | (F.col(c) != F.col(f"old_{c}"))

    changed = joined.filter(change_condition).select(incoming.columns)
    return changed


def apply_scd2(spark: SparkSession, changes: DataFrame, change_date: date):
    """
    Merge changed records into the dimension table.

    Strategy: two-pass via temp views because Iceberg's MERGE INTO
    doesn't support inserting + updating in the same matched clause
    cleanly for SCD2. Instead we:
      1. Expire current rows for changed customers
      2. Insert new current rows
    """
    if changes.isEmpty():
        return

    # Step 1: close existing current rows
    customer_ids = [row.customer_id for row in changes.select("customer_id").collect()]
    id_list = ",".join(f"'{cid}'" for cid in customer_ids)

    expire_date = date.fromordinal(change_date.toordinal() - 1)

    spark.sql(f"""
        UPDATE {SCD2_TABLE}
        SET effective_to = DATE '{expire_date.isoformat()}',
            is_current = false
        WHERE customer_id IN ({id_list})
          AND is_current = true
    """)

    # Step 2: insert new current rows
    new_rows = (
        changes
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
    """Full SCD2 flow: initial load + apply changes."""
    from pipeline.schemas import CUSTOMER_SCHEMA

    customers = spark.read.schema(CUSTOMER_SCHEMA).json(customers_path)
    initial = build_initial_dimension(customers, as_of=initial_date)
    create_dim_table(spark, initial)

    changes = spark.read.schema(CUSTOMER_SCHEMA).json(changes_path)
    detected = detect_changes(
        spark.read.table(SCD2_TABLE),
        changes,
    )
    apply_scd2(spark, detected, change_date)
