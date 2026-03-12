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