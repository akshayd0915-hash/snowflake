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