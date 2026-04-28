"""
Canonical schemas for each pipeline stage. Defined once, imported everywhere.
"""

from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
)

RAW_TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=True),
    StructField("customer_id", StringType(), nullable=True),
    StructField("amount", DecimalType(18, 2), nullable=True),
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

# Snapshot rates for USD normalization. In production this would be a reference
# table keyed by (currency, date) from a market data feed.
FX_RATES_USD = {
    "USD": 1.0,
    "EUR": 1.08,
    "GBP": 1.27,
    "INR": 0.012,
}
FX_RATES_AS_OF = "2026-04-01"
