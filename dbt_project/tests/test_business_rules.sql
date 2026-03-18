-- ============================================================
-- BUSINESS RULE VALIDATIONS
-- These tests return rows when a rule is VIOLATED
-- dbt expects 0 rows for a passing test
-- ============================================================

-- Rule 1: Outstanding loan balance cannot exceed principal amount
-- A balance > principal indicates a data error
SELECT
    loan_id,
    customer_id,
    principal_amount,
    outstanding_balance,
    'Outstanding balance exceeds principal' as violation
FROM {{ ref('mart_loan_risk') }}
WHERE outstanding_balance > principal_amount