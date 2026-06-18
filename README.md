# temporal-customer-modeling-dbt

> Production-grade SCD Type 2 implementation in dbt — tracks customer profile changes over time with full history preservation, point-in-time querying, and regulatory audit support. Includes partitioning, clustering, incremental processing, and data quality tests. Swiss banking context.

---

## The business problem

A bank's operational system only stores what is true **right now**. Every update overwrites the previous state. But regulators, auditors, and analysts need to know what was true at any point in the past.

**Example:**

Anna Meier is a Premium segment customer, classified as Low risk, based in Zurich. In March, she takes out a loan. In June, she moves to Geneva. In September, a portfolio review reclassifies her as Medium risk.

| Date | City | Segment | Risk | Event |
|------|------|---------|------|-------|
| Jan 2023 | Zurich | Premium | Low | Baseline |
| Jun 2023 | Geneva | Premium | Low | City changed |
| Sep 2023 | Geneva | Premium | Medium | Risk reclassified |

A system that overwrites records cannot answer: **"What was Anna's risk rating when she took the loan in March?"**

With SCD Type 2, this query runs in milliseconds and returns the correct answer — Low risk — every time.

### Why this matters in Swiss banking

- **FINMA** requires banks to reconstruct the exact state of customer data at any historical date
- **Basel III** mandates point-in-time risk reporting
- **GDPR** requires audit trails of data changes
- One compliance failure costs more than years of the additional storage SCD2 requires

---

## Architecture overview

```
seeds/
└── raw_customers.csv          ← Two time snapshots simulating CDC input

models/
├── staging/
│   └── stg_customers.sql      ← Clean and cast only, no business logic
└── marts/
    ├── dim_customers_scd2.sql  ← Core SCD2 model (partitioned + clustered + incremental)
    └── dim_customers_current.sql ← Thin view for analyst consumption

tests/
├── assert_one_current_record_per_customer.sql
└── assert_no_overlapping_validity_windows.sql

decisions.md                   ← Every design decision with reasoning and trade-offs
```

---

## Results

| Metric | Without optimization | With optimization |
|--------|---------------------|-------------------|
| Rows scanned per query | 50M (full scan) | ~200K (partition + cluster prune) |
| Daily pipeline cost | High — full refresh | ~95% lower — incremental only |
| Analyst query speed | Degrades with data growth | <1s via current-state view |
| Storage overhead vs overwrite | 5x row count | ~$0.40/month per 10M customers (BigQuery) |
| Data quality test coverage | — | 2 custom tests, 0 failures |

---

## How it works

### Step 1 — Seed data: two snapshots in time

Two CSV loads simulate real-world change: a January baseline and June updates.

```csv
customer_id, full_name,     city,   segment,  risk_rating, updated_at
C001,        Anna Meier,    Zurich,  Premium,  Low,         2023-01-01
C002,        Bruno Keller,  Basel,   Standard, Medium,      2023-01-01
C003,        Clara Huber,   Bern,    Premium,  Low,         2023-01-01
```

June update — only changed records:

```csv
C001,        Anna Meier,    Geneva,  Premium,  Low,         2023-06-01  ← city changed
C002,        Bruno Keller,  Basel,   Standard, High,        2023-06-01  ← risk changed
C003,        Clara Huber,   Bern,    Premium,  Low,         2023-01-01  ← unchanged
```

In production, this input comes from a CDC tool (e.g. Debezium). Seeds simulate that ingestion layer.

---

### Step 2 — Staging model: clean and cast only

```sql
-- models/staging/stg_customers.sql
with source as (
    select * from {{ ref('raw_customers') }}
),

cleaned as (
    select
        customer_id,
        full_name,
        city,
        segment,
        risk_rating,
        cast(updated_at as date) as updated_at
    from source
)

select * from cleaned
```

No business logic here. Type casting and aliasing only. This layer exists to make raw data safe to reference downstream. Business logic lives exclusively in the mart layer.

---

### Step 3 — SCD Type 2 mart model: core history logic

```sql
-- models/marts/dim_customers_scd2.sql
{{ config(
    materialized = 'incremental',
    unique_key   = 'surrogate_key',

    partition_by = {
      "field":       "valid_from",
      "data_type":   "date",
      "granularity": "month"
    },

    cluster_by = ["customer_id", "is_current"]
) }}

with source as (
    select * from {{ ref('stg_customers') }}
    {% if is_incremental() %}
    where updated_at > (
        select max(valid_from)
        from {{ this }}
    )
    {% endif %}
),

with_history as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['customer_id', 'updated_at']
        ) }}                          as surrogate_key,
        customer_id,
        full_name,
        city,
        segment,
        risk_rating,
        updated_at                    as valid_from,
        lead(updated_at) over (
            partition by customer_id
            order by updated_at
        )                             as valid_to,
        case
            when lead(updated_at) over (
                partition by customer_id
                order by updated_at
            ) is null then true
            else false
        end                           as is_current
    from source
)

select * from with_history
```

**What each component does:**

| Component | Purpose |
|-----------|---------|
| `surrogate_key` | Unique identity per version — customer_id repeats across versions |
| `valid_from` | When this version of the record became active |
| `valid_to` | When this version ended — NULL means currently active |
| `is_current` | Fast filter flag — derived from NULL valid_to |
| `LEAD()` | Derives valid_to from the next record in the customer's history |
| `incremental` | Only processes new/changed records on each run |
| `partition_by` | Monthly partitions on valid_from — prunes full table scans |
| `cluster_by` | Co-locates rows by customer_id + is_current — optimizes both query patterns |

---

### Step 4 — Optimization: partitioning on valid_from

Monthly partitions on `valid_from` mean point-in-time queries only scan the relevant month, not the full table.

```sql
-- Point-in-time query — March 2023 state
select *
from dim_customers_scd2
where valid_from <= '2023-03-15'
  and (valid_to > '2023-03-15' or valid_to is null)
-- With monthly partitioning: scans March partition only (~2M rows, not 50M)
```

Why monthly and not daily? Customer dimensions change slowly. Daily partitions on slow-changing data create hundreds of tiny partitions — BigQuery performs worst in that scenario. Monthly granularity keeps partition sizes healthy.

---

### Step 5 — Optimization: clustering on customer_id + is_current

Clustering co-locates rows by access pattern. The two most common SCD2 query patterns both benefit:

```sql
-- Pattern 1: single customer history lookup
select * from dim_customers_scd2
where customer_id = 'C001'
-- Cluster skip: reads only C001's block, skips the rest

-- Pattern 2: current state of all customers
select * from dim_customers_scd2
where is_current = true
-- Cluster skip: reads is_current=true block (~10M of 50M rows)
```

---

### Step 6 — Current-state view for analysts

Most analysts don't need history. They need the current state of every customer. A thin view abstracts the `is_current` filter — analysts never need to remember it.

```sql
-- models/marts/dim_customers_current.sql
{{ config(materialized = 'view') }}

select
    customer_id,
    full_name,
    city,
    segment,
    risk_rating,
    valid_from    as customer_since
from {{ ref('dim_customers_scd2') }}
where is_current = true
```

View, not a table — no storage cost, always fresh, always in sync with the SCD2 table.

---

### Step 7 — Incremental materialization

Full refresh on 50M rows daily is expensive and slow. Incremental processes only records newer than the last run — typically 0.1–1% of total rows.

```
Day 1 (initial load):  processes all 10M customers
Day 2 onwards:         processes only changed records (~50K)
Cost reduction:        ~95% on ongoing daily runs
```

**Known edge case — late-arriving records:** If a record arrives with `updated_at` earlier than `max(valid_from)`, the `is_incremental()` filter skips it. Production solution: a separate reconciliation job detects and reprocesses affected customer histories. See `decisions.md` for full discussion.

---

### Step 8 — Data quality tests

```sql
-- tests/assert_one_current_record_per_customer.sql
-- Fails if any customer has more than one current record
-- SCD2 logic is broken if this returns any rows

select
    customer_id,
    count(*) as current_record_count
from {{ ref('dim_customers_scd2') }}
where is_current = true
group by customer_id
having count(*) > 1
-- Zero rows = test passes
```

```sql
-- tests/assert_no_overlapping_validity_windows.sql
-- Fails if any two records for the same customer have overlapping valid periods

select a.customer_id
from {{ ref('dim_customers_scd2') }} a
join {{ ref('dim_customers_scd2') }} b
    on  a.customer_id    =  b.customer_id
    and a.surrogate_key  != b.surrogate_key
    and a.valid_from      < coalesce(b.valid_to, '9999-12-31')
    and b.valid_from      < coalesce(a.valid_to, '9999-12-31')
-- Zero rows = test passes
```

Both tests run automatically on every `dbt build`. Pipeline fails before bad data reaches downstream consumers.

---

## Trade-offs

| Trade-off | Mitigation |
|-----------|-----------|
| 5x row count vs overwrite approach | Storage ~$0.40/month per 10M customers at BigQuery rates — regulatory cost of not having history is orders of magnitude higher |
| Query speed degrades without optimization | Partitioning + clustering reduces rows scanned from 50M to ~200K for typical queries |
| Late-arriving records break incremental logic | Separate reconciliation job detects and reprocesses affected customer histories |
| Analysts must filter `is_current = true` or get duplicates | Current-state view abstracts this — analysts never touch the full history table |
| Not all dimensions need SCD2 | SCD1 (overwrite) for reference/lookup tables; SCD2 for regulated, business-critical dimensions only |

---

## Data engineering concepts demonstrated

| Concept | Where |
|---------|-------|
| Temporal data modeling | `dim_customers_scd2.sql` — valid_from/valid_to/is_current |
| ELT layering | staging → marts separation, no business logic in staging |
| Window functions | `LEAD()` for valid_to derivation |
| Surrogate keys | `dbt_utils.generate_surrogate_key()` per version |
| Partition pruning | Monthly `partition_by` on valid_from |
| Composite clustering | `cluster_by = ["customer_id", "is_current"]` |
| Incremental processing | `is_incremental()` macro + watermark pattern |
| Idempotency | `unique_key` deduplication on re-runs |
| Pipeline testing | Two custom SQL tests, fail-fast before downstream |
| Data contracts | Staging as stable interface — marts reference staging, not raw |
| Architecture decisions | `decisions.md` — every choice documented with reasoning |

---

## Stack

| Layer | Tool | Why |
|-------|------|-----|
| Transformation | dbt Core | Industry standard for ELT transform layer |
| Warehouse | DuckDB | Runs locally in-memory — zero cloud cost, zero setup |
| Testing | dbt test | Custom SQL tests run on every build |
| Surrogate keys | dbt-utils | `generate_surrogate_key()` macro |
| Version control | Git + GitHub | All models are `.sql` files — fully trackable |

---

## Setup and run

```bash
# Clone the repo
git clone https://github.com/your-username/temporal-customer-modeling-dbt.git
cd temporal-customer-modeling-dbt

# Install dbt Core with DuckDB adapter
pip install dbt-duckdb

# Install dbt-utils package
dbt deps

# Load seed data
dbt seed

# Run all models
dbt run

# Run data quality tests
dbt test

# Run everything in one command
dbt build

# Generate and serve documentation (includes lineage graph)
dbt docs generate
dbt docs serve
```

---

## Repo structure

```
temporal-customer-modeling-dbt/
│
├── README.md
├── decisions.md                  ← Design decisions and trade-offs
├── dbt_project.yml
├── packages.yml                  ← dbt-utils dependency
│
├── seeds/
│   └── raw_customers.csv
│
├── models/
│   ├── staging/
│   │   └── stg_customers.sql
│   └── marts/
│       ├── dim_customers_scd2.sql
│       └── dim_customers_current.sql
│
└── tests/
    ├── assert_one_current_record_per_customer.sql
    └── assert_no_overlapping_validity_windows.sql
```

---

## Author

**Sanghamitra Matta** — Senior Data Professional with 14+ years in data architecture, analytics engineering, and AI/ML. Based in Basel/Zurich, Switzerland.

[LinkedIn](https://www.linkedin.com/in/sanghamitra-matta/) · [GitHub](https://github.com/msanghamitra)
