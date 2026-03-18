{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

with enriched as (
    select * from {{ ref('int_transaction_enriched') }}

    {% if is_incremental() %}
    where transaction_date > (
        select coalesce(max(latest_transaction_date), '1900-01-01'::date)
        from {{ this }}
    )
    {% endif %}
),

monthly_agg as (
    select
        transaction_id,
        transaction_month,
        customer_id,
        channel,
        category,
        amount,
        is_debit,
        is_flagged,
        transaction_date                                    as latest_transaction_date,
        current_timestamp()                                 as dbt_updated_at
    from enriched
)

select * from monthly_agg