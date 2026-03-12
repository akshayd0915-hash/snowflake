with source as (
    select * from {{ source('raw', 'customers') }}
),

renamed as (
    select
        customer_id,
        first_name,
        last_name,
        first_name || ' ' || last_name   as full_name,
        lower(email)                      as email,
        phone,
        state,
        segment,
        date_of_birth,
        datediff('year', date_of_birth, current_date()) as age,
        created_date,
        is_active,
        current_timestamp()              as dbt_loaded_at
    from source
)

select * from renamed