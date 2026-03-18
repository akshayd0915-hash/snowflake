-- ============================================================
-- ANOMALY DETECTION
-- Flags transactions that are statistical outliers
-- Returns rows where amount > 3 standard deviations from mean
-- ============================================================

WITH stats AS (
    SELECT
        AVG(amount)    as mean_amount,
        STDDEV(amount) as stddev_amount
    FROM {{ ref('stg_transactions') }}
),
anomalies AS (
    SELECT
        t.transaction_id,
        t.customer_id,
        t.amount,
        t.transaction_date,
        t.channel,
        s.mean_amount,
        s.stddev_amount,
        (t.amount - s.mean_amount) / NULLIF(s.stddev_amount, 0) as z_score,
        'Transaction amount is a statistical outlier (z-score > 3)' as violation
    FROM {{ ref('stg_transactions') }} t
    CROSS JOIN stats s
    WHERE ABS((t.amount - s.mean_amount) / NULLIF(s.stddev_amount, 0)) > 3
)
SELECT * FROM anomalies