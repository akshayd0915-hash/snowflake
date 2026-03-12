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