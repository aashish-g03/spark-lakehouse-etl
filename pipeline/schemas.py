"""
Canonical schemas for each pipeline stage. Defined once, imported everywhere.
"""

from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

RAW_TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=True),
    StructField("customer_id", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("currency", StringType(), nullable=True),
    StructField("transaction_type", StringType(), nullable=True),
    StructField("merchant_category", StringType(), nullable=True),
    StructField("event_date", StringType(), nullable=True),
    StructField("event_timestamp", StringType(), nullable=True),
    StructField("channel", StringType(), nullable=True),  # added day 15+
])

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("address", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("tier", StringType(), nullable=True),
])

VALID_CURRENCIES = {"USD", "EUR", "GBP", "INR"}
VALID_TXN_TYPES = {"deposit", "withdrawal", "transfer", "payment"}
