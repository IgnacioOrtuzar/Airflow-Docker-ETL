{{ config(materialized='view') }}

with Source_order as (
 
    select * 

    from {{source('client_b','orders_b')}}

),


Source_stores as (
 
    select * 

    from {{source('client_b','stores_b')}}

),

order_process as (

    select

    id_order,
    id_customer,
    date_order,
    st.name_store,
    st.country_store

    from Source_order o
    left join Source_stores st on o.id_store = st.id_store

)


select * from order_process
