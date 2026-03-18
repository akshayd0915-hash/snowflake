-- ============================================================
-- ANOMALY DETECTION
-- Flags transactions that are statistical outliers
-- Returns rows where amount > 3 standard deviations from mean
-- Configured as warn (not error) since anomalies are expected
-- ============================================================

{{ config(severity='warn') }}

with stats as (
    select
        avg(amount)    as mean_amount,
        stddev(amount) as stddev_amount
    from {{ ref('stg_transactions') }}
),
anomalies as (
    select
        t.transaction_id,
        t.customer_id,
        t.amount,
        t.transaction_date,
        t.channel,
        s.mean_amount,
        s.stddev_amount,
        (t.amount - s.mean_amount) / nullif(s.stddev_amount, 0) as z_score,
        'Transaction amount is a statistical outlier (z-score > 3)' as violation
    from {{ ref('stg_transactions') }} t
    cross join stats s
    where abs((t.amount - s.mean_amount) / nullif(s.stddev_amount, 0)) > 3
)
select * from anomalies