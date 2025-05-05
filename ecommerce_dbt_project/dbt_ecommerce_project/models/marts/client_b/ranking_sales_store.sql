{{ config(materialized='table') }}

with orders as (
    select 

    id_order,
    name_store

    from {{ref('stg_orders_b')}}
),

order_items as (

    select
    id_order,
    quantity,
    item_price

    from {{ref('stg_order_item_b')}}
),

order_x_orders_items as (

    select 
    name_store,
    sum(quantity * item_price) as total_sales
    from orders o
    left join order_items oi on o.id_order = oi.id_order
    group by name_store
),
ranking_sales as (
    select
        name_store,
        total_sales,
        rank() over (order by total_sales desc) as sales_rank
    from order_x_orders_items
)

select * from ranking_sales