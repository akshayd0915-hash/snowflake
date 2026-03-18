import snowflake.connector
import csv
import os

# ── connection ────────────────────────────────────────────
conn = snowflake.connector.connect(
    account   = "mec15672.us-east-1",
    user      = "AKSHAYDUBEY401",
    password  = input("Enter your Snowflake password: "),
    warehouse = "COMPUTE_WH",
    database  = "BANKING_DW",
    schema    = "RAW",
    role      = "ACCOUNTADMIN"
)
cursor = conn.cursor()
print("Connected to Snowflake successfully!\n")

# ── table definitions ─────────────────────────────────────
tables = {
    "customers": """
        CREATE OR REPLACE TABLE BANKING_DW.RAW.customers (
            customer_id     VARCHAR,
            first_name      VARCHAR,
            last_name       VARCHAR,
            email           VARCHAR,
            phone           VARCHAR,
            state           VARCHAR,
            segment         VARCHAR,
            date_of_birth   DATE,
            created_date    DATE,
            is_active       BOOLEAN
        )""",

    "accounts": """
        CREATE OR REPLACE TABLE BANKING_DW.RAW.accounts (
            account_id      VARCHAR,
            customer_id     VARCHAR,
            account_type    VARCHAR,
            balance         FLOAT,
            credit_limit    FLOAT,
            interest_rate   FLOAT,
            account_status  VARCHAR,
            opened_date     DATE,
            closed_date     DATE
        )""",

    "transactions": """
        CREATE OR REPLACE TABLE BANKING_DW.RAW.transactions (
            transaction_id      VARCHAR,
            account_id          VARCHAR,
            customer_id         VARCHAR,
            transaction_date    DATE,
            amount              FLOAT,
            transaction_type    VARCHAR,
            channel             VARCHAR,
            category            VARCHAR,
            description         VARCHAR,
            is_debit            BOOLEAN
        )""",

    "loans": """
        CREATE OR REPLACE TABLE BANKING_DW.RAW.loans (
            loan_id                 VARCHAR,
            customer_id             VARCHAR,
            loan_type               VARCHAR,
            principal_amount        FLOAT,
            outstanding_balance     FLOAT,
            interest_rate           FLOAT,
            monthly_payment         FLOAT,
            origination_date        DATE,
            maturity_date           DATE,
            loan_status             VARCHAR,
            debt_to_income_ratio    FLOAT
        )""",

    "fraud_flags": """
        CREATE OR REPLACE TABLE BANKING_DW.RAW.fraud_flags (
            fraud_id            VARCHAR,
            transaction_id      VARCHAR,
            customer_id         VARCHAR,
            fraud_type          VARCHAR,
            fraud_score         FLOAT,
            fraud_status        VARCHAR,
            flagged_date        DATE,
            reviewed_by         VARCHAR,
            resolution_notes    VARCHAR
        )"""
}

# ── create tables ─────────────────────────────────────────
for table_name, ddl in tables.items():
    cursor.execute(ddl)
    print(f"Created table: RAW.{table_name}")

# ── load CSVs ─────────────────────────────────────────────
csv_files = {
    "customers":    "ingestion/customers.csv",
    "accounts":     "ingestion/accounts.csv",
    "transactions": "ingestion/transactions.csv",
    "loans":        "ingestion/loans.csv",
    "fraud_flags":  "ingestion/fraud_flags.csv"
}

for table_name, filepath in csv_files.items():
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        rows   = list(reader)

    if not rows:
        print(f"Skipping {table_name} — no data found")
        continue

    placeholders = ", ".join(["%s"] * len(rows[0]))
    insert_sql   = f"INSERT INTO BANKING_DW.RAW.{table_name} VALUES ({placeholders})"

    data = [tuple(None if v == '' else v for v in row.values()) for row in rows]
    cursor.executemany(insert_sql, data)
    print(f"Loaded {len(data)} rows into RAW.{table_name}")

# ── verify ────────────────────────────────────────────────
print("\nRow counts in RAW schema:")
for table_name in tables.keys():
    cursor.execute(f"SELECT COUNT(*) FROM BANKING_DW.RAW.{table_name}")
    count = cursor.fetchone()[0]
    print(f"  RAW.{table_name}: {count} rows")

cursor.close()
conn.close()
print("\nAll done!")