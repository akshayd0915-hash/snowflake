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