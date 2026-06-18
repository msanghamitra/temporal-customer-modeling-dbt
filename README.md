# temporal-customer-modeling-dbt
Production-grade SCD Type 2 implementation in dbt - tracks customer profile changes over time with full history preservation, point-in-time querying, and regulatory audit support. Includes partitioning, clustering, incremental processing, and data quality tests. Swiss banking context.

# The Business problem

A bank's operational system only stores what is true right now. Every update overwrites the previous state. But regulators, auditors, and analysts need to know what was true at any point in the past.

Example:

Anna Meier is a Premium segment customer, classified as Low risk, based in Zurich. In March, she takes out a loan. In June, she moves to Geneva. In September, a portfolio review reclassifies her as Medium risk.

Date     City    Segment      Risk      Event
Jan 2023 Zurich  Premium      Low       Baseline
Jun 2023 Geneva  Premium      Low       City changed
Sep 2023 Geneva  Premium      Medium    Risk reclassified

A system that overwrites records cannot answer: "What was Anna's risk rating when she took the loan in March?"

With SCD Type 2, this query runs in milliseconds and returns the correct answer — Low risk — every time.

Why this matters in Swiss banking


FINMA requires banks to reconstruct the exact state of customer data at any historical date
Basel III mandates point-in-time risk reporting
GDPR requires audit trails of data changes
One compliance failure costs more than years of the additional storage SCD2 requires



# Architecture overview

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



