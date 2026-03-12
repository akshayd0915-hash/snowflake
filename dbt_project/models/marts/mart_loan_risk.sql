{{
    config(materialized='table')
}}

with loan_risk as (
    select * from {{ ref('int_loan_risk') }}
),
final as (
    select
        loan_id,
        customer_id,
        loan_type,
        principal_amount,
        outstanding_balance,
        ltv_ratio,
        interest_rate_pct,
        monthly_payment,
        origination_date,
        maturity_date,
        loan_status,
        debt_to_income_ratio,
        dti_risk_flag,
        risk_score,
        is_at_risk,
        case
            when risk_score = 4 then 'CRITICAL'
            when risk_score = 3 then 'HIGH'
            when risk_score = 2 then 'MEDIUM'
            else 'LOW'
        end                         as risk_tier,
        current_timestamp()         as dbt_updated_at
    from loan_risk
)
select * from final