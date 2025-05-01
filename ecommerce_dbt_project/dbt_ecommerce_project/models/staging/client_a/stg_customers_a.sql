{{ config(materialized='view') }}

with source as (

    select * 
    from {{ source('client_a', 'customers_a') }}

),

renamed as (

    select
        customer_id,
        first_name,
        last_name,
        email,
        country,
        created_at
    from source

)

select * from renamed