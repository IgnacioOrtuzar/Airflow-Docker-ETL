{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders_a') }}
),

customers as (
    select * from {{ ref('stg_customers_a') }}
),

customers_x_total_orders as (
    select

        o.customer_id,
        count(*) as total_orders
    from orders o
    left join customers c
        on o.customer_id = c.customer_id
    group by o.customer_id
)

select * from customers_x_total_orders

