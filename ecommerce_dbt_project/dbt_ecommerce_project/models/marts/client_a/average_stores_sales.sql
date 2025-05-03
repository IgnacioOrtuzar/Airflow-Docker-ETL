{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders_a') }}
),

orders_items as (
    select * from {{ ref('stg_order_item_a') }}
),

stores_x_quantity_items_sales as (
    select 
        o.store_name,
        sum(oi.quantity) as total_quantity
    from orders o
    left join orders_items oi on o.order_id = oi.order_id
    group by o.store_name
    order by total_quantity desc
)

select * from stores_x_quantity_items_sales
