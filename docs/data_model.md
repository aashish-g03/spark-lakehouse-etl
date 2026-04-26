# Data Model

## Entity Relationship

```
                         ┌─────────────────────┐
                         │    dim_customer      │
                         │  (SCD Type 2)        │
                         ├─────────────────────┤
                         │ customer_sk  (SK)    │
                         │ customer_id  (NK) ◄──┼────┐
                         │ name                 │    │
                         │ email                │    │
                         │ address              │    │
                         │ city                 │    │
                         │ country              │    │
                         │ tier                 │    │
                         │ effective_from       │    │
                         │ effective_to         │    │
                         │ is_current           │    │
                         └─────────────────────┘    │
                                                     │
┌─────────────────────────────────┐                  │
│      fact_transactions          │                  │
│  (partitioned by event_date)    │                  │
├─────────────────────────────────┤                  │
│ transaction_id  (SK, UUID)      │                  │
│ customer_id     (FK) ───────────┼──────────────────┘
│ amount                          │
│ amount_usd                      │
│ currency                        │
│ transaction_type                │
│ merchant_category               │
│ event_date      (partition key) │
│ event_timestamp                 │
│ channel                         │
│ customer_name   (denormalized)  │
│ customer_country                │
│ customer_tier                   │
└─────────────────────────────────┘
```

## Key Design Decisions

### Surrogate vs Natural Keys

- `transaction_id` (fact): surrogate key (UUID). Transactions don't have a natural business key that's guaranteed unique across source systems.
- `customer_id` (dimension): natural key. Stable identifier from the source system, used for joins.
- `customer_sk` (dimension): surrogate key per SCD2 version. Needed because the same `customer_id` can have multiple rows.

### Why SCD Type 2 (not Type 1 or Type 3)

Type 1 (overwrite) loses history. Type 3 (add columns) only tracks one prior state. Type 2 keeps full history, which is what you need for questions like:
- "What was this customer's tier when they made transaction X?"
- "How many customers were in the gold tier on March 15th?"
- "Show me all tier changes for customer Y over time"

The trade-off is more rows in the dimension table, but customer dimensions are small relative to facts so the overhead is negligible.

### Partition Strategy

`fact_transactions` is partitioned by `event_date`:
- Financial queries almost always have a date range filter
- Each partition corresponds to one day of data, keeping file sizes manageable
- Avoids the small-file problem that would occur with high-cardinality partitions (e.g., customer_id would create 10K partitions with tiny files)

`dim_customer` is not partitioned. It's small enough (~10K rows with SCD2 overhead) that full scans are fast.

### Schema Evolution

The `channel` field is absent from the first 15 days of data and present from day 16 onward. The Iceberg table handles this transparently through schema evolution. Older records have `null` for channel. No ETL code changes were needed to handle the addition.

### Denormalization in Fact Table

`customer_name`, `customer_country`, and `customer_tier` are denormalized into the fact table during the transform stage. This is a deliberate trade-off:
- Avoids a join at query time for the most common analytics queries
- Acceptable because these fields change infrequently (and SCD2 tracks the changes separately)
- The values reflect the customer state at load time, not at transaction time
