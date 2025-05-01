{{ config(materialized='view') }}

with Customer_b_data as (
    select *
    from {{source('client_b','customers_b')}}

),

customer_data_process as(
    
    select 
    id as customer_id,
    name as customer_name,
    email_address,
    registration_date as created_at

    from Customer_b_data
    
)

select * from customer_data_process

