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
        coalesce(ca.total_balance, 0)       as total_balance,
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