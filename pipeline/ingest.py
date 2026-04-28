"""
Bronze layer: read raw JSON, enforce schema, quarantine bad records.

The ingest stage is intentionally strict — anything that doesn't pass validation
gets routed to a quarantine table so we can audit it later without blocking the
pipeline. Clean records move forward with proper types.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, TimestampType

from pipeline.schemas import RAW_TRANSACTION_SCHEMA, VALID_CURRENCIES, VALID_TXN_TYPES


def read_raw_transactions(spark: SparkSession, path: str) -> DataFrame:
    return (
        spark.read
        .schema(RAW_TRANSACTION_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt")
        .json(path)
    )


def validate(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Split raw records into (clean, quarantine).

    Quarantine reasons:
      - null transaction_id or customer_id
      - null or negative amount
      - unrecognized currency code or transaction type
      - event_date that parses to null or is in the future
    """
    df = df.withColumn("_event_date", F.to_date(F.col("event_date")))
    df = df.withColumn("_event_ts", F.to_timestamp(F.col("event_timestamp")))

    is_valid = (
        F.col("transaction_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("amount").isNotNull()
        & (F.col("amount") > 0)
        & F.col("currency").isin(list(VALID_CURRENCIES))
        & F.col("transaction_type").isin(list(VALID_TXN_TYPES))
        & F.col("_event_date").isNotNull()
        & (F.col("_event_date") <= F.current_date())
    )

    clean = (
        df.filter(is_valid)
        .drop("event_date", "event_timestamp")
        .withColumnRenamed("_event_date", "event_date")
        .withColumnRenamed("_event_ts", "event_timestamp")
    )

    quarantine = df.filter(~is_valid).drop("_event_date", "_event_ts")

    return clean, quarantine


def ingest(spark: SparkSession, raw_path: str) -> tuple[DataFrame, DataFrame]:
    raw = read_raw_transactions(spark, raw_path)
    return validate(raw)
