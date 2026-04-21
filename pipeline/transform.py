"""
Silver layer: deduplicate, enrich with customer dimension via broadcast join.

Expects the output of ingest.validate() — typed, quarantine-free records.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.schemas import CUSTOMER_SCHEMA


def load_customers(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.schema(CUSTOMER_SCHEMA).json(path)


def deduplicate(df: DataFrame) -> DataFrame:
    """Drop exact duplicates on transaction_id, keeping the first occurrence."""
    return df.dropDuplicates(["transaction_id"])


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
    """Computed columns useful for downstream analytics."""
    return (
        df
        .withColumn("amount_usd",
                     F.when(F.col("currency") == "USD", F.col("amount"))
                     .when(F.col("currency") == "EUR", F.col("amount") * 1.08)
                     .when(F.col("currency") == "GBP", F.col("amount") * 1.27)
                     .when(F.col("currency") == "INR", F.col("amount") * 0.012)
                     .otherwise(F.col("amount")))
        .withColumn("amount_usd", F.round("amount_usd", 2))
    )


def transform(
    spark: SparkSession,
    clean_transactions: DataFrame,
    customers_path: str,
) -> DataFrame:
    customers = load_customers(spark, customers_path)
    deduped = deduplicate(clean_transactions)
    enriched = enrich(deduped, customers)
    return add_derived_columns(enriched)
