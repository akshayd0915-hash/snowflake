import os

files = {}

files["models/staging/stg_transactions.sql"] = """
with source as (
    select * from {{ source('raw', 'transactions') }}
),
renamed as (
    select
        transaction_id,
        account_id,
        customer_id,
        transaction_date,
        date_trunc('month', transaction_date) as transaction_month,
        round(amount, 2)                      as amount,
        case
            when is_debit = true then amount * -1
            else amount
        end                                   as signed_amount,
        transaction_type,
        channel,
        category,
        description,
        is_debit,
        current_timestamp()                   as dbt_loaded_at
    from source
)
select * from renamed
""".strip()

files["models/staging/stg_loans.sql"] = """
with source as (
    select * from {{ source('raw', 'loans') }}
),
renamed as (
    select
        loan_id,
        customer_id,
        loan_type,
        round(principal_amount, 2)      as principal_amount,
        round(outstanding_balance, 2)   as outstanding_balance,
        round(interest_rate * 100, 4)   as interest_rate_pct,
        round(monthly_payment, 2)       as monthly_payment,
        origination_date,
        maturity_date,
        loan_status,
        round(debt_to_income_ratio, 4)  as debt_to_income_ratio,
        case
            when debt_to_income_ratio >= 0.43 then 'HIGH'
            when debt_to_income_ratio >= 0.36 then 'MEDIUM'
            else 'LOW'
        end                             as dti_risk_flag,
        current_timestamp()             as dbt_loaded_at
    from source
)
select * from renamed
""".strip()

files["models/staging/stg_fraud_flags.sql"] = """
with source as (
    select * from {{ source('raw', 'fraud_flags') }}
),
renamed as (
    select
        fraud_id,
        transaction_id,
        customer_id,
        fraud_type,
        round(fraud_score, 4)   as fraud_score,
        case
            when fraud_score >= 0.8 then 'HIGH'
            when fraud_score >= 0.65 then 'MEDIUM'
            else 'LOW'
        end                     as fraud_risk_level,
        fraud_status,
        flagged_date,
        reviewed_by,
        resolution_notes,
        current_timestamp()     as dbt_loaded_at
    from source
)
select * from renamed
""".strip()

files["models/staging/sources.yml"] = """
version: 2

sources:
  - name: raw
    database: BANKING_DW
    schema: RAW
    tables:
      - name: customers
        description: Raw customer data
      - name: accounts
        description: Raw account data
      - name: transactions
        description: Raw transaction data
      - name: loans
        description: Raw loan data
      - name: fraud_flags
        description: Raw fraud flag data
""".strip()

files["models/intermediate/int_customer_accounts.sql"] = """
with customers as (
    select * from {{ ref('stg_customers') }}
),
accounts as (
    select * from {{ ref('stg_accounts') }}
),
joined as (
    select
        c.customer_id,
        c.full_name,
        c.email,
        c.state,
        c.segment,
        c.age,
        c.is_active,
        count(a.account_id)             as total_accounts,
        sum(a.balance)                  as total_balance,
        sum(a.credit_limit)             as total_credit_limit,
        sum(case when a.is_open then 1 else 0 end) as open_accounts,
        max(a.account_age_days)         as oldest_account_days
    from customers c
    left join accounts a on c.customer_id = a.customer_id
    group by 1,2,3,4,5,6,7
)
select * from joined
""".strip()

files["models/intermediate/int_transaction_enriched.sql"] = """
with transactions as (
    select * from {{ ref('stg_transactions') }}
),
fraud as (
    select * from {{ ref('stg_fraud_flags') }}
),
enriched as (
    select
        t.transaction_id,
        t.account_id,
        t.customer_id,
        t.transaction_date,
        t.transaction_month,
        t.amount,
        t.signed_amount,
        t.transaction_type,
        t.channel,
        t.category,
        t.is_debit,
        case when f.fraud_id is not null then true else false end  as is_flagged,
        f.fraud_type,
        f.fraud_score,
        f.fraud_risk_level,
        f.fraud_status
    from transactions t
    left join fraud f on t.transaction_id = f.transaction_id
)
select * from enriched
""".strip()

files["models/intermediate/int_loan_risk.sql"] = """
with loans as (
    select * from {{ ref('stg_loans') }}
),
risk as (
    select
        loan_id,
        customer_id,
        loan_type,
        principal_amount,
        outstanding_balance,
        round((outstanding_balance / nullif(principal_amount, 0)) * 100, 2) as ltv_ratio,
        interest_rate_pct,
        monthly_payment,
        origination_date,
        maturity_date,
        loan_status,
        debt_to_income_ratio,
        dti_risk_flag,
        case
            when loan_status = 'DEFAULT'     then 4
            when loan_status = 'DELINQUENT'  then 3
            when loan_status = 'CURRENT'     then 1
            else 2
        end                             as risk_score,
        case
            when loan_status in ('DEFAULT', 'DELINQUENT') then true
            else false
        end                             as is_at_risk
    from loans
)
select * from risk
""".strip()

files["models/marts/mart_customer_360.sql"] = """
{{
    config(
        materialized='table',
        unique_key='customer_id'
    )
}}

with customer_accounts as (
    select * from {{ ref('int_customer_accounts') }}
),
loan_summary as (
    select
        customer_id,
        count(*)                        as total_loans,
        sum(outstanding_balance)        as total_loan_balance,
        sum(case when is_at_risk then 1 else 0 end) as at_risk_loans,
        avg(debt_to_income_ratio)       as avg_dti
    from {{ ref('int_loan_risk') }}
    group by 1
),
fraud_summary as (
    select
        customer_id,
        count(distinct transaction_id)  as flagged_transactions,
        max(fraud_score)                as max_fraud_score
    from {{ ref('stg_fraud_flags') }}
    group by 1
),
final as (
    select
        ca.customer_id,
        ca.full_name,
        ca.email,
        ca.state,
        ca.segment,
        ca.age,
        ca.is_active,
        ca.total_accounts,
        ca.open_accounts,
        ca.total_balance,
        ca.total_credit_limit,
        ca.oldest_account_days,
        coalesce(ls.total_loans, 0)         as total_loans,
        coalesce(ls.total_loan_balance, 0)  as total_loan_balance,
        coalesce(ls.at_risk_loans, 0)       as at_risk_loans,
        coalesce(ls.avg_dti, 0)             as avg_dti,
        coalesce(fs.flagged_transactions, 0) as flagged_transactions,
        coalesce(fs.max_fraud_score, 0)     as max_fraud_score,
        case
            when coalesce(fs.max_fraud_score, 0) >= 0.8 then 'HIGH'
            when coalesce(fs.max_fraud_score, 0) >= 0.65 then 'MEDIUM'
            else 'LOW'
        end                                 as fraud_risk_level,
        current_timestamp()                 as dbt_updated_at
    from customer_accounts ca
    left join loan_summary ls   on ca.customer_id = ls.customer_id
    left join fraud_summary fs  on ca.customer_id = fs.customer_id
)
select * from final
""".strip()

files["models/marts/mart_transaction_analytics.sql"] = """
{{
    config(materialized='table')
}}

with enriched as (
    select * from {{ ref('int_transaction_enriched') }}
),
monthly_agg as (
    select
        transaction_month,
        customer_id,
        channel,
        category,
        count(*)                        as total_transactions,
        sum(amount)                     as total_amount,
        avg(amount)                     as avg_amount,
        max(amount)                     as max_amount,
        sum(case when is_debit then amount else 0 end)  as total_debits,
        sum(case when not is_debit then amount else 0 end) as total_credits,
        sum(case when is_flagged then 1 else 0 end)     as flagged_count
    from enriched
    group by 1,2,3,4
)
select * from monthly_agg
""".strip()

files["models/marts/mart_loan_risk.sql"] = """
{{
    config(materialized='table')
}}

with loan_risk as (
    select * from {{ ref('int_loan_risk') }}
),
final as (
    select
        loan_id,
        customer_id,
        loan_type,
        principal_amount,
        outstanding_balance,
        ltv_ratio,
        interest_rate_pct,
        monthly_payment,
        origination_date,
        maturity_date,
        loan_status,
        debt_to_income_ratio,
        dti_risk_flag,
        risk_score,
        is_at_risk,
        case
            when risk_score = 4 then 'CRITICAL'
            when risk_score = 3 then 'HIGH'
            when risk_score = 2 then 'MEDIUM'
            else 'LOW'
        end                         as risk_tier,
        current_timestamp()         as dbt_updated_at
    from loan_risk
)
select * from final
""".strip()

files["models/marts/mart_fraud_detection.sql"] = """
{{
    config(materialized='table')
}}

with fraud as (
    select * from {{ ref('int_transaction_enriched') }}
    where is_flagged = true
),
summary as (
    select
        customer_id,
        count(distinct transaction_id)      as total_flagged_txns,
        sum(amount)                         as total_flagged_amount,
        avg(fraud_score)                    as avg_fraud_score,
        max(fraud_score)                    as max_fraud_score,
        count(distinct fraud_type)          as distinct_fraud_types,
        sum(case when fraud_risk_level = 'HIGH' then 1 else 0 end)   as high_risk_count,
        sum(case when fraud_risk_level = 'MEDIUM' then 1 else 0 end) as medium_risk_count,
        max(transaction_date)               as latest_fraud_date
    from fraud
    group by 1
)
select * from summary
""".strip()

files["models/marts/schema.yml"] = """
version: 2

models:
  - name: mart_customer_360
    description: Complete customer profile with account, loan and fraud summary
    columns:
      - name: customer_id
        description: Primary key
        tests:
          - unique
          - not_null
      - name: total_balance
        tests:
          - not_null

  - name: mart_transaction_analytics
    description: Monthly transaction aggregations per customer
    columns:
      - name: transaction_month
        tests:
          - not_null

  - name: mart_loan_risk
    description: Loan risk tier and metrics per loan
    columns:
      - name: loan_id
        tests:
          - unique
          - not_null

  - name: mart_fraud_detection
    description: Fraud summary per customer
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null
""".strip()

# Write all files
base = r"C:\Users\PC\Documents\snowflake\dbt_project"
for path, content in files.items():
    full_path = os.path.join(base, path)
    os.makedirs(os.path.dirname(full_path), exist_ok=True)
    with open(full_path, "w") as f:
        f.write(content)
    print(f"Created: {path}")

print("\nAll files created successfully!")