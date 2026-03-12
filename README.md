# 🏦 Banking Data Platform — Snowflake + dbt

A production-style banking data platform built with Snowflake and dbt Core, demonstrating end-to-end ELT pipeline design, dimensional data modeling, and data quality practices for a regulated financial services environment.

---

## 📌 Project Overview

This project simulates a real banking data engineering workload covering:

- **Customer 360** — unified customer profile with account, loan and fraud data
- **Transaction Analytics** — monthly aggregations by channel, category and customer
- **Loan Risk Analysis** — DTI ratio, LTV, risk tiering and delinquency tracking
- **Fraud Detection** — fraud scoring, risk levels and customer-level fraud summaries

---

## 📊 Data Lineage Graph

![dbt Lineage Graph](docs/lineage_graph.png)

---

## 🏗️ Architecture

| Layer | Tool | Description |
|---|---|---|
| 1. Source | Python | CSV files generated with realistic banking data |
| 2. Raw | Snowflake | Unmodified source tables loaded via Python ELT script |
| 3. Staging | dbt views | `stg_*` models — cast, rename, clean raw data |
| 4. Intermediate | dbt views | `int_*` models — joins and business logic |
| 5. Marts | dbt tables | `mart_*` models — star schema, aggregations, risk tiers |
| 6. Consumption | BI / Compliance | Analytics, reporting, audit trails |

---

## 📂 Project Structure
```
snowflake/
├── ingestion/
│   ├── generate_data.py
│   └── load_to_snowflake.py
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── snapshots/
│   │   └── scd2_customers.sql
│   └── dbt_project.yml
├── docs/
│   └── lineage_graph.png
├── .github/
│   └── workflows/
│       └── dbt_ci.yml
└── README.md
```

---

## 🔧 Tech Stack

| Tool | Purpose |
|---|---|
| Snowflake | Cloud data warehouse — storage, compute, security |
| dbt Core | Data transformation, testing, documentation, lineage |
| Python | Data generation and ELT ingestion pipeline |
| GitHub Actions | CI/CD — runs dbt run and dbt test on every push |
| Git + GitHub | Version control and portfolio hosting |

---

## 📊 Data Model

| Layer | Models | Materialization |
|---|---|---|
| Staging | stg_customers, stg_accounts, stg_transactions, stg_loans, stg_fraud_flags | Views |
| Intermediate | int_customer_accounts, int_transaction_enriched, int_loan_risk | Views |
| Marts | mart_customer_360, mart_transaction_analytics, mart_loan_risk, mart_fraud_detection | Tables |
| Snapshots | scd2_customers | SCD2 table |

### Key modeling concepts demonstrated

- **Star schema** — fact and dimension separation in mart layer
- **SCD2** — full customer history tracking with valid_from / valid_to timestamps
- **Risk tiering** — DTI ratio, LTV, fraud scoring with business rule logic
- **Null handling** — coalesce patterns for outer join safety
- **Data quality tests** — not_null and unique constraints on all primary keys

---

## 🏦 Banking Domain Coverage

| Business Area | dbt Model | Key Metrics |
|---|---|---|
| Customer 360 | mart_customer_360 | Total balance, credit limit, fraud risk level |
| Transaction Analytics | mart_transaction_analytics | Monthly spend, channel mix, flagged count |
| Loan Risk | mart_loan_risk | DTI ratio, LTV, risk tier, delinquency flag |
| Fraud Detection | mart_fraud_detection | Fraud score, risk level, flagged transactions |
| Customer History | scd2_customers | Full SCD2 history with valid_from / valid_to |

---

## ✅ Data Quality

- 8 automated dbt tests across all mart models
- not_null tests on all primary and critical business keys
- unique tests on customer_id, loan_id, fraud customer_id
- All tests passing: PASS=8 WARN=0 ERROR=0

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

**4. Generate and load data**
```bash
python ingestion/generate_data.py
python ingestion/load_to_snowflake.py
```

**5. Run dbt models**
```bash
cd dbt_project
dbt run
dbt test
```

**6. View data lineage**
```bash
dbt docs generate
dbt docs serve
```

---

## 📈 Data Volume

| Table | Rows |
|---|---|
| RAW.customers | 200 |
| RAW.accounts | 350 |
| RAW.transactions | 2,000 |
| RAW.loans | 150 |
| RAW.fraud_flags | 100 |

---

## 👤 Author

Built by Akshay Dubey as a portfolio project targeting data engineering roles in the banking and financial services domain.
