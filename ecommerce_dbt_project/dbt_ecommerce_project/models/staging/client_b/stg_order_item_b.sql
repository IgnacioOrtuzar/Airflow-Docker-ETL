{{ config(materialized='view') }}

with Source_order_items as (
 
    select * 

    from {{source('client_b','order_items_b')}}

),

Source_warehouse as (
 
    select * 

    from {{source('client_b','warehouses_b')}}

),
order_items_process as (

        select
        id_order,
        id_product,
        item_price,
        cast(qty as INTEGER) as quantity,
        wh.name_warehouse,
        wh.country_warehouse


        from Source_order_items  oi
        left join Source_warehouse  wh
        on oi.id_warehouse = wh.id_warehouse
)

select * from order_items_process