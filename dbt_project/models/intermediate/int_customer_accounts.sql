with customers as (
    select * from {{ ref('stg_customers') }}
),
accounts as (
    select * from {{ ref('stg_accounts') }}
),
joined as (
    select
        c.customer_id,
        c.full_name,
        c.email,
        c.state,
        c.segment,
        c.age,
        c.is_active,
        count(a.account_id)             as total_accounts,
        sum(a.balance)                  as total_balance,
        sum(a.credit_limit)             as total_credit_limit,
        sum(case when a.is_open then 1 else 0 end) as open_accounts,
        max(a.account_age_days)         as oldest_account_days
    from customers c
    left join accounts a on c.customer_id = a.customer_id
    group by 1,2,3,4,5,6,7
)
select * from joined