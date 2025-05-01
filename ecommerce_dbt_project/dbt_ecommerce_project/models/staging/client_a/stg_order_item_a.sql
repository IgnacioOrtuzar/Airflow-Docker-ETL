{{ config(materialized='view') }}

with Source_order_items as (
 
    select * 

    from {{source('client_a','order_items_a')}}

),

Source_warehouse as (
 
    select * 

    from {{source('client_a','warehouses_a')}}

),
order_items_process as (

        select
        order_id,
        product_id,
        quantity,
        wh.warehouse_name,
        wh.warehouse_country


        from Source_order_items as oi
        left join Source_warehouse as wh
        on oi.warehouse_id = wh.warehouse_id 

)

select * from order_items_process