# spark-lakehouse-etl

Batch ETL pipeline built with Apache Spark and Apache Iceberg. Ingests synthetic financial transactions, enforces data quality, writes to a partitioned Iceberg lakehouse, maintains a Slowly Changing Dimension (Type 2) for customer state, and reconciles output against source manifests.

## Architecture

```
JSON files (synthetic financial transactions)
  |
  v
[Ingest] -- schema validation, type casting, bad-record quarantine
  |
  v
[Transform] -- deduplication, broadcast join with customers, currency normalization
  |
  v
[Write] -- Iceberg fact_transactions table, partitioned by event_date
  |
  v
[SCD Type 2] -- dim_customer: track tier/address changes over time
  |
  v
[Reconcile] -- row count + sum(amount) vs source manifest, flag discrepancies
```

## Data Model

### fact_transactions (Iceberg, partitioned by `event_date`)

| Column | Type | Notes |
|--------|------|-------|
| transaction_id | string | Surrogate key (UUID) |
| customer_id | string | FK to dim_customer |
| amount | double | Transaction amount in original currency |
| amount_usd | double | Normalized to USD |
| currency | string | ISO currency code |
| transaction_type | string | deposit, withdrawal, transfer, payment |
| merchant_category | string | Spending category |
| event_date | date | Partition key |
| event_timestamp | timestamp | |
| channel | string | mobile, web, branch, atm (added day 15 for schema evolution) |
| customer_name | string | Enriched from dim_customer |
| customer_country | string | Enriched from dim_customer |
| customer_tier | string | Enriched from dim_customer |

**Partition strategy:** Partitioned by `event_date` because financial queries almost always filter by date range. High-cardinality columns like `customer_id` would create too many small files.

### dim_customer (Iceberg, SCD Type 2)

| Column | Type | Notes |
|--------|------|-------|
| customer_sk | string | Surrogate key (UUID, new per version) |
| customer_id | string | Natural key (stable across versions) |
| name, email, address, city, country | string | |
| tier | string | bronze/silver/gold/platinum (tracked for changes) |
| effective_from | date | When this version became active |
| effective_to | date | 9999-12-31 for current rows |
| is_current | boolean | Convenience flag |

When a customer's tier or address changes, the current row is expired (`effective_to` set to change date minus one, `is_current` set to false) and a new row is inserted. This preserves full history for temporal queries.

**Key design choice:** `customer_sk` is a surrogate key generated per version. `customer_id` is the natural key that stays stable across all versions of the same customer. The fact table joins on `customer_id` (not `customer_sk`) because transactions reference the customer entity, not a specific version of it. Point-in-time lookups filter on `effective_from <= query_date AND effective_to >= query_date`.

## Data Quality

The pipeline enforces quality at the ingest stage rather than post-hoc:

- **Null checks:** transaction_id, customer_id, amount must be non-null
- **Range checks:** amount must be positive
- **Referential integrity:** currency must be in the valid set (USD, EUR, GBP, INR)
- **Temporal integrity:** event_date must parse and not be in the future
- **Schema evolution:** the `channel` field is optional (added on day 15 of generated data)

Bad records are quarantined to a separate output for auditing rather than silently dropped.

## Reconciliation

After the pipeline writes to Iceberg, we recompute `row_count` and `sum(amount)` per day from the output table and compare against the source manifest. Expected discrepancies (quarantined + deduplicated records) are accounted for. Unexpected mismatches are flagged.

Output: `data/output/reconciliation_report.json`

## Quick Start

```bash
# Setup
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Generate test data (~10K transactions)
python data_generator.py --small

# Run pipeline
python main.py

# Run tests
pytest tests/ -v
```

**Requirements:** Python 3.10+, Java 11 or 17 (Spark dependency)

### Full dataset (~10M transactions)

```bash
python data_generator.py            # generates 30 days, ~333K txn/day
python main.py                      # takes a few minutes on a laptop
```

## Project Structure

```
spark-lakehouse-etl/
  main.py                 # pipeline orchestrator
  data_generator.py       # Faker-based synthetic data + manifest
  pipeline/
    spark_session.py      # shared Spark + Iceberg session config
    schemas.py            # canonical schemas for each stage
    ingest.py             # raw -> bronze (validate + quarantine)
    transform.py          # bronze -> silver (dedup, join, derive)
    scd2.py               # SCD Type 2 merge for dim_customer
    reconcile.py          # source vs sink reconciliation
  tests/
    conftest.py           # shared Spark session fixture
    test_ingest.py        # 7 tests: validation rules, schema evolution
    test_transform.py     # 6 tests: dedup, join, currency conversion
    test_scd2.py          # 4 tests: initial load, change detection, expire+insert
    test_reconcile.py     # 2 tests: manifest loading, report structure
  docs/
    data_model.md         # ER diagram and key design decisions
    lineage.md            # data flow documentation
```

## Tech Stack

- **PySpark 3.5** - distributed data processing
- **Apache Iceberg 1.7** - table format with schema evolution, time travel, ACID transactions
- **Faker** - synthetic data generation with controlled quality defects
- **chispa** - DataFrame assertion library for Spark tests
- **pytest** - test framework

## Author

[Aashish Gupta](https://linkedin.com/in/aashish03) · Software Engineer @ [Leena AI](https://leena.ai) · [aashishgupta.tech](https://www.aashishgupta.tech)
