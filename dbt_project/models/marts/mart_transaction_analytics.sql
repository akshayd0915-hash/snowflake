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