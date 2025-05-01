{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders_a') }}
),

customers as (
    select * from {{ ref('stg_customers_a') }}
),

joined as (
    select
        o.customer_id,
        count(*) as total_orders
    from orders o
    left join customers c
        on o.customer_id = c.customer_id
    group by o.customer_id
)

select * from joined
