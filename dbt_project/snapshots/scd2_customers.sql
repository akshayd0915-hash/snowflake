{% snapshot scd2_customers %}

{{
    config(
        target_schema='DBT_DEV',
        unique_key='customer_id',
        strategy='check',
        check_cols=[
            'email',
            'phone',
            'state',
            'segment',
            'is_active'
        ]
    )
}}

select
    customer_id,
    full_name,
    email,
    phone,
    state,
    segment,
    age,
    is_active,
    dbt_loaded_at
from {{ ref('stg_customers') }}

{% endsnapshot %}