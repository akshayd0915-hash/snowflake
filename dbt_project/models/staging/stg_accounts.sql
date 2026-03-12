with source as (
    select * from {{ source('raw', 'accounts') }}
),

renamed as (
    select
        account_id,
        customer_id,
        account_type,
        round(balance, 2)                as balance,
        round(credit_limit, 2)           as credit_limit,
        round(interest_rate * 100, 4)    as interest_rate_pct,
        account_status,
        opened_date,
        closed_date,
        case
            when closed_date is not null then false
            else true
        end                              as is_open,
        datediff('day', opened_date, current_date()) as account_age_days,
        current_timestamp()              as dbt_loaded_at
    from source
)

select * from renamed