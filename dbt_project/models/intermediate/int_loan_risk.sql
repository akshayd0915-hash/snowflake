with loans as (
    select * from {{ ref('stg_loans') }}
),
risk as (
    select
        loan_id,
        customer_id,
        loan_type,
        principal_amount,
        outstanding_balance,
        round((outstanding_balance / nullif(principal_amount, 0)) * 100, 2) as ltv_ratio,
        interest_rate_pct,
        monthly_payment,
        origination_date,
        maturity_date,
        loan_status,
        debt_to_income_ratio,
        dti_risk_flag,
        case
            when loan_status = 'DEFAULT'     then 4
            when loan_status = 'DELINQUENT'  then 3
            when loan_status = 'CURRENT'     then 1
            else 2
        end                             as risk_score,
        case
            when loan_status in ('DEFAULT', 'DELINQUENT') then true
            else false
        end                             as is_at_risk
    from loans
)
select * from risk