with source as (
    select * from {{ source('raw', 'loans') }}
),
renamed as (
    select
        loan_id,
        customer_id,
        loan_type,
        round(principal_amount, 2)      as principal_amount,
        round(outstanding_balance, 2)   as outstanding_balance,
        round(interest_rate * 100, 4)   as interest_rate_pct,
        round(monthly_payment, 2)       as monthly_payment,
        origination_date,
        maturity_date,
        loan_status,
        round(debt_to_income_ratio, 4)  as debt_to_income_ratio,
        case
            when debt_to_income_ratio >= 0.43 then 'HIGH'
            when debt_to_income_ratio >= 0.36 then 'MEDIUM'
            else 'LOW'
        end                             as dti_risk_flag,
        current_timestamp()             as dbt_loaded_at
    from source
)
select * from renamed