{{ config(materialized='view') }}

with Source_order as (
 
    select * 

    from {{source('client_a','orders_a')}}

),


Source_stores as (
 
    select * 

    from {{source('client_a','stores_a')}}

),

order_process as (

    select

    order_id,
    customer_id,
    order_date,
    st.store_name,
    st.store_country

    from Source_order o
    left join Source_stores st on o.store_id = st.store_id

)


select * from order_process
