"""
Silver layer: deduplicate, enrich with customer dimension via broadcast join.

Expects the output of ingest.validate() — typed, quarantine-free records.
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pipeline.schemas import CUSTOMER_SCHEMA, FX_RATES_USD


def load_customers(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.schema(CUSTOMER_SCHEMA).json(path)


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Deduplicate on transaction_id, keeping the latest record by event_timestamp.
    Using a window function instead of dropDuplicates so the result is
    deterministic and reproducible across runs.
    """
    window = Window.partitionBy("transaction_id").orderBy(
        F.col("event_timestamp").desc()
    )
    return (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def enrich(transactions: DataFrame, customers: DataFrame) -> DataFrame:
    """
    Broadcast join with customer dimension. Customers table is small (~10K rows),
    so broadcast avoids a shuffle on the fact side.
    """
    customer_cols = customers.select(
        F.col("customer_id"),
        F.col("name").alias("customer_name"),
        F.col("country").alias("customer_country"),
        F.col("tier").alias("customer_tier"),
    )

    return transactions.join(
        F.broadcast(customer_cols),
        on="customer_id",
        how="left",
    )


def add_derived_columns(df: DataFrame) -> DataFrame:
    """Normalize amounts to USD using reference FX rates."""
    fx_expr = F.col("amount")  # fallback: keep original
    for currency, rate in FX_RATES_USD.items():
        fx_expr = F.when(
            F.col("currency") == currency, F.col("amount") * F.lit(rate)
        ).otherwise(fx_expr)

    return df.withColumn("amount_usd", F.round(fx_expr, 2))


def transform(
    spark: SparkSession,
    clean_transactions: DataFrame,
    customers_path: str,
) -> DataFrame:
    customers = load_customers(spark, customers_path)
    deduped = deduplicate(clean_transactions)
    enriched = enrich(deduped, customers)
    return add_derived_columns(enriched)
