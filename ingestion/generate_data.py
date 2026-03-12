import csv
import random
from datetime import datetime, timedelta

random.seed(42)

# ── helpers ──────────────────────────────────────────────
def rand_date(start_year=2020, end_year=2024):
    start = datetime(start_year, 1, 1)
    end   = datetime(end_year, 12, 31)
    return (start + timedelta(days=random.randint(0, (end - start).days))).strftime("%Y-%m-%d")

def write_csv(filename, headers, rows):
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Created {filename} with {len(rows)} rows")

# ── customers ─────────────────────────────────────────────
first_names = ["James","Mary","John","Patricia","Robert","Jennifer","Michael","Linda","William","Barbara"]
last_names  = ["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Wilson","Taylor"]
states      = ["NY","CA","TX","FL","IL","PA","OH","GA","NC","MI"]
segments    = ["RETAIL","PREMIUM","PRIVATE","SMALL_BUSINESS"]

customers = []
for i in range(1, 201):
    customers.append({
        "customer_id":   f"CUST{i:04d}",
        "first_name":    random.choice(first_names),
        "last_name":     random.choice(last_names),
        "email":         f"customer{i}@email.com",
        "phone":         f"555-{random.randint(1000,9999)}",
        "state":         random.choice(states),
        "segment":       random.choice(segments),
        "date_of_birth": rand_date(1960, 1995),
        "created_date":  rand_date(2020, 2022),
        "is_active":     random.choice([True, True, True, False])
    })
write_csv("ingestion/customers.csv", list(customers[0].keys()), customers)

# ── accounts ──────────────────────────────────────────────
account_types   = ["CHECKING","SAVINGS","MONEY_MARKET","CD"]
account_status  = ["ACTIVE","ACTIVE","ACTIVE","CLOSED","FROZEN"]

accounts = []
for i in range(1, 351):
    cust = random.choice(customers)
    accounts.append({
        "account_id":      f"ACC{i:05d}",
        "customer_id":     cust["customer_id"],
        "account_type":    random.choice(account_types),
        "balance":         round(random.uniform(100, 150000), 2),
        "credit_limit":    round(random.uniform(1000, 50000), 2),
        "interest_rate":   round(random.uniform(0.01, 0.08), 4),
        "account_status":  random.choice(account_status),
        "opened_date":     rand_date(2020, 2022),
        "closed_date":     rand_date(2023, 2024) if random.random() < 0.1 else None
    })
write_csv("ingestion/accounts.csv", list(accounts[0].keys()), accounts)

# ── transactions ──────────────────────────────────────────
txn_types       = ["DEPOSIT","WITHDRAWAL","TRANSFER","PAYMENT","FEE"]
txn_channels    = ["ONLINE","BRANCH","ATM","MOBILE","WIRE"]
txn_categories  = ["SALARY","RENT","GROCERIES","UTILITIES","ENTERTAINMENT","TRANSFER","OTHER"]

transactions = []
for i in range(1, 2001):
    acc = random.choice(accounts)
    transactions.append({
        "transaction_id":   f"TXN{i:06d}",
        "account_id":       acc["account_id"],
        "customer_id":      acc["customer_id"],
        "transaction_date": rand_date(2022, 2024),
        "amount":           round(random.uniform(1, 10000), 2),
        "transaction_type": random.choice(txn_types),
        "channel":          random.choice(txn_channels),
        "category":         random.choice(txn_categories),
        "description":      f"Transaction {i}",
        "is_debit":         random.choice([True, False])
    })
write_csv("ingestion/transactions.csv", list(transactions[0].keys()), transactions)

# ── loans ─────────────────────────────────────────────────
loan_types      = ["MORTGAGE","AUTO","PERSONAL","HOME_EQUITY","STUDENT"]
loan_status     = ["CURRENT","CURRENT","CURRENT","DELINQUENT","DEFAULT","PAID_OFF"]

loans = []
for i in range(1, 151):
    cust = random.choice(customers)
    principal = round(random.uniform(5000, 500000), 2)
    loans.append({
        "loan_id":              f"LOAN{i:04d}",
        "customer_id":          cust["customer_id"],
        "loan_type":            random.choice(loan_types),
        "principal_amount":     principal,
        "outstanding_balance":  round(principal * random.uniform(0.1, 1.0), 2),
        "interest_rate":        round(random.uniform(0.03, 0.18), 4),
        "monthly_payment":      round(principal * random.uniform(0.01, 0.03), 2),
        "origination_date":     rand_date(2020, 2022),
        "maturity_date":        rand_date(2025, 2035),
        "loan_status":          random.choice(loan_status),
        "debt_to_income_ratio": round(random.uniform(0.1, 0.6), 4)
    })
write_csv("ingestion/loans.csv", list(loans[0].keys()), loans)

# ── fraud flags ───────────────────────────────────────────
fraud_types     = ["CARD_NOT_PRESENT","ACCOUNT_TAKEOVER","IDENTITY_THEFT","UNUSUAL_PATTERN","VELOCITY_BREACH"]
fraud_status    = ["CONFIRMED","SUSPECTED","CLEARED","UNDER_REVIEW"]

fraud_flags = []
fraud_txns  = random.sample(transactions, 100)
for i, txn in enumerate(fraud_txns, 1):
    fraud_flags.append({
        "fraud_id":           f"FRD{i:04d}",
        "transaction_id":     txn["transaction_id"],
        "customer_id":        txn["customer_id"],
        "fraud_type":         random.choice(fraud_types),
        "fraud_score":        round(random.uniform(0.5, 1.0), 4),
        "fraud_status":       random.choice(fraud_status),
        "flagged_date":       txn["transaction_date"],
        "reviewed_by":        f"ANALYST{random.randint(1,10):02d}",
        "resolution_notes":   f"Case {i} under review"
    })
write_csv("ingestion/fraud_flags.csv", list(fraud_flags[0].keys()), fraud_flags)

print("\nAll CSV files generated successfully!")