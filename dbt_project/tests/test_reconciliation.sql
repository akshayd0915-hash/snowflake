-- ============================================================
-- RECONCILIATION TEST
-- Validates that mart_customer_360 has same customer count as RAW
-- Returns rows if counts don't match — indicates pipeline failure
-- ============================================================

WITH raw_count AS (
    SELECT COUNT(DISTINCT customer_id) as cnt
    FROM {{ source('raw', 'customers') }}
),
mart_count AS (
    SELECT COUNT(DISTINCT customer_id) as cnt
    FROM {{ ref('mart_customer_360') }}
)
SELECT
    raw_count.cnt   as raw_customer_count,
    mart_count.cnt  as mart_customer_count,
    'Customer count mismatch between RAW and mart' as violation
FROM raw_count, mart_count
WHERE raw_count.cnt != mart_count.cnt