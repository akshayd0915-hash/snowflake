# 🏦 Banking Data Platform — Snowflake + dbt

A production-grade banking data platform built with Snowflake, dbt Core, Python, and GitHub Actions CI/CD. Designed to demonstrate end-to-end data engineering capabilities for a regulated financial services environment.

---

## 📊 Data Lineage Graph

![dbt Lineage Graph](docs/lineage_graph.png)

---

## 📌 Project Overview

This project simulates a real banking data engineering workload covering:

- **Customer 360** — unified customer profile with account, loan and fraud data
- **Transaction Analytics** — incremental monthly aggregations by channel and category
- **Loan Risk Analysis** — DTI ratio, LTV, risk tiering and delinquency tracking
- **Fraud Detection** — real-time fraud scoring using Snowflake Streams and Tasks

---

## 🏗️ Architecture

| Layer | Tool | Description |
|---|---|---|
| 1. Source | Python | Synthetic banking CSVs generated with realistic data |
| 2. Raw | Snowflake | Unmodified source tables loaded via Python ELT script |
| 3. Staging | dbt views | `stg_*` models — cast, rename, clean raw data |
| 4. Intermediate | dbt views | `int_*` models — joins and business logic |
| 5. Marts | dbt tables | `mart_*` models — star schema, risk tiers, aggregations |
| 6. Snapshots | dbt snapshot | SCD2 customer history tracking |
| 7. Streaming | Snowflake Streams + Tasks | Near real-time fraud alert processing |
| 8. Consumption | Power BI | Executive dashboards on curated mart tables |

---

## 📂 Project Structure
```
snowflake/
├── ingestion/
│   ├── generate_data.py              # Generates synthetic banking CSV data
│   ├── load_to_snowflake.py          # Loads CSVs into Snowflake RAW schema
│   └── stream_fraud_simulator.py     # Simulates real-time fraud transactions
├── dbt_project/
│   ├── models/
│   │   ├── staging/                  # stg_* models — clean and standardize
│   │   ├── intermediate/             # int_* models — joins and business logic
│   │   └── marts/                    # mart_* models — analytics ready tables
│   ├── snapshots/
│   │   └── scd2_customers.sql        # SCD2 customer history tracking
│   └── tests/
│       ├── test_business_rules.sql   # Loan balance validation
│       ├── test_reconciliation.sql   # RAW vs mart row count check
│       └── test_anomaly_detection.sql # Statistical outlier detection
├── setup/
│   ├── rbac_setup.sql                # Role-based access control setup
│   └── snowflake_tasks.sql           # Pipeline orchestration with Tasks
├── docs/
│   └── lineage_graph.png             # dbt data lineage screenshot
├── .github/
│   └── workflows/
│       └── dbt_ci.yml                # CI/CD — runs dbt on every push
└── README.md
```

---

## 🔧 Tech Stack

| Tool | Purpose |
|---|---|
| Snowflake | Cloud data warehouse — storage, compute, RBAC, Streams, Tasks |
| dbt Core | Transformation, testing, documentation, lineage, SCD2 snapshots |
| Python | Data generation, ELT ingestion, streaming simulation |
| GitHub Actions | CI/CD — runs dbt run + dbt test on every push to main |
| Power BI | BI consumption layer — dashboards on curated mart tables |
| Git + GitHub | Version control and portfolio hosting |

---

## 📊 Data Model

| Layer | Models | Materialization | Rows |
|---|---|---|---|
| Staging | stg_customers, stg_accounts, stg_transactions, stg_loans, stg_fraud_flags | Views | — |
| Intermediate | int_customer_accounts, int_transaction_enriched, int_loan_risk | Views | — |
| Marts | mart_customer_360, mart_transaction_analytics, mart_loan_risk, mart_fraud_detection | Tables | 200 / 2000+ / 150 / 73 |
| Snapshots | scd2_customers | SCD2 table | 200+ |

### Key modeling concepts demonstrated

- **Star schema** — fact and dimension separation in mart layer
- **SCD2** — full customer history with valid_from / valid_to timestamps
- **Incremental loading** — merge strategy with timestamp watermarking on transactions
- **Risk tiering** — DTI ratio, LTV, fraud scoring with business rule classification
- **Null safety** — coalesce patterns for outer join handling
- **Data quality** — 11 automated tests: standard + business rules + anomaly detection

---

## 🏦 Banking Domain Coverage

| Business Area | Model | Key Metrics |
|---|---|---|
| Customer 360 | mart_customer_360 | Total balance, credit limit, fraud risk level, loan summary |
| Transaction Analytics | mart_transaction_analytics | Monthly spend, channel mix, debit/credit split, fraud flags |
| Loan Risk | mart_loan_risk | DTI ratio, LTV, risk tier (CRITICAL/HIGH/MEDIUM/LOW) |
| Fraud Detection | mart_fraud_detection | Fraud score, risk level, flagged transaction count |
| Customer History | scd2_customers | Full SCD2 audit trail for segment and state changes |
| Real-time Fraud | fraud_alerts_realtime | Stream-based alerts for high-value and wire transactions |

---

## 🔒 Security & Governance

Role-based access control following least privilege principle:

| Role | Access | Purpose |
|---|---|---|
| DBT_TRANSFORMER | RAW read + DBT_DEV write | dbt service account — never ACCOUNTADMIN |
| BANKING_ANALYST | All mart tables read | Analytics team |
| BANKING_READER | Customer + Transaction marts only | Business users — no access to sensitive fraud/loan data |

---

## ⚙️ Orchestration

Snowflake Task chain simulating production pipeline scheduling:
```
TASK_PIPELINE_MONITOR (hourly)
    → TASK_DATA_QUALITY_CHECK (validates RAW row counts)
        → TASK_FRESHNESS_CHECK (validates mart update recency)
```

In production: replaced with Apache Airflow using SnowflakeOperator
to orchestrate ingest → dbt run → dbt test → snapshot with retry logic.

---

## 🌊 Streaming Architecture

| Component | Purpose |
|---|---|
| TRANSACTIONS_STREAM | Snowflake Stream (CDC) capturing new transaction inserts |
| TASK_FRAUD_STREAM_PROCESSOR | Runs every minute, processes stream when data arrives |
| FRAUD_ALERTS_REALTIME | Landing table for classified fraud alerts |
| stream_fraud_simulator.py | Python script simulating micro-batch fraud transactions |

Alert classification rules:
- `HIGH_VALUE_TRANSACTION` — amount > $8,000
- `WIRE_TRANSFER_ALERT` — channel = WIRE
- `UNUSUAL_PATTERN` — all other flagged transactions

---

## ✅ Data Quality

| Test Type | Tests | Status |
|---|---|---|
| not_null | customer_id, total_balance, loan_id, transaction_month, fraud customer_id | ✅ Passing |
| unique | customer_id, loan_id, fraud customer_id | ✅ Passing |
| Business rules | Loan balance cannot exceed principal amount | ✅ Passing |
| Reconciliation | Customer count must match between RAW and mart | ✅ Passing |
| Anomaly detection | Transactions with z-score > 3 flagged for review | ✅ Passing |

Total: 11 automated tests — PASS=11 WARN=0 ERROR=0

---

## 🚀 How to Run

**1. Clone the repo**
```bash
git clone https://github.com/akshayd0915-hash/snowflake.git
cd snowflake
```

**2. Install dependencies**
```bash
pip install dbt-snowflake snowflake-connector-python
```

**3. Configure dbt profile** — create `~/.dbt/profiles.yml`:
```yaml
dbt_project:
  outputs:
    dev:
      type: snowflake
      account: <your_account>
      user: <your_username>
      password: <your_password>
      role: ACCOUNTADMIN
      warehouse: COMPUTE_WH
      database: BANKING_DW
      schema: DBT_DEV
      threads: 4
  target: dev
```

**4. Set up Snowflake — run in Snowflake worksheet:**
```sql
CREATE DATABASE IF NOT EXISTS BANKING_DW;
CREATE SCHEMA IF NOT EXISTS BANKING_DW.RAW;
CREATE SCHEMA IF NOT EXISTS BANKING_DW.DBT_DEV;
```

**5. Generate and load data**
```bash
python ingestion/generate_data.py
python ingestion/load_to_snowflake.py
```

**6. Run dbt models**
```bash
cd dbt_project
dbt run
dbt test
dbt snapshot
```

**7. Simulate streaming fraud**
```bash
python ingestion/stream_fraud_simulator.py
```

**8. View data lineage**
```bash
dbt docs generate
dbt docs serve
```

---

## 📈 Data Volume

| Table | Layer | Rows |
|---|---|---|
| RAW.customers | Raw | 200 |
| RAW.accounts | Raw | 350 |
| RAW.transactions | Raw | 2,000+ |
| RAW.loans | Raw | 150 |
| RAW.fraud_flags | Raw | 100 |
| mart_customer_360 | Mart | 200 |
| mart_transaction_analytics | Mart | 2,000+ (incremental) |
| mart_loan_risk | Mart | 150 |
| mart_fraud_detection | Mart | 73 |
| scd2_customers | Snapshot | 200+ (grows with changes) |
| fraud_alerts_realtime | Streaming | 15+ (grows with simulator) |

---

## 👤 Author

Built by Akshay Dubey as a portfolio project targeting senior data engineering roles in the banking and financial services domain.
