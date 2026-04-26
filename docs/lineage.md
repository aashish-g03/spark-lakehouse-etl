# Data Lineage

## Pipeline Stages

```
data_generator.py
  |
  ├── data/raw/customers.json          (10K customer records)
  ├── data/raw/customer_changes.json   (~500 tier/address mutations)
  ├── data/raw/transactions/day=*/     (JSON-per-day, ~333K records each)
  └── data/raw/manifest.json           (row counts + sums for reconciliation)

main.py
  |
  ├── STAGE 1: Ingest (pipeline/ingest.py)
  │   Input:  data/raw/transactions/day=*/*.json
  │   Output: clean DataFrame + quarantine DataFrame
  │   Rules:  schema validation, null checks, range checks, temporal checks
  │   Reject: data/output/quarantine/ (JSON, for auditing)
  │
  ├── STAGE 2: Transform (pipeline/transform.py)
  │   Input:  clean DataFrame + data/raw/customers.json
  │   Output: enriched DataFrame
  │   Steps:  deduplicate on transaction_id
  │           broadcast join with customer dimension
  │           add amount_usd derived column
  │
  ├── STAGE 3: Write (main.py)
  │   Input:  enriched DataFrame
  │   Output: local.lakehouse.fact_transactions (Iceberg)
  │   Config: partitioned by event_date
  │
  ├── STAGE 4: SCD2 (pipeline/scd2.py)
  │   Input:  data/raw/customers.json + data/raw/customer_changes.json
  │   Output: local.lakehouse.dim_customer (Iceberg)
  │   Logic:  initial load, then detect and apply tier/address changes
  │
  └── STAGE 5: Reconcile (pipeline/reconcile.py)
      Input:  local.lakehouse.fact_transactions + data/raw/manifest.json
      Output: data/output/reconciliation_report.json
      Check:  row_count and sum(amount) per day, source vs sink
```

## Data Flow Per Record

```
Raw JSON record
  -> Read with explicit schema (ingest.py)
  -> Validate: null checks, type checks, range checks
  -> [PASS] -> Deduplicate on transaction_id (transform.py)
             -> Broadcast join with customer dimension
             -> Add derived columns (amount_usd)
             -> Write to Iceberg fact table
  -> [FAIL] -> Write to quarantine (JSON, preserves original record)
```
