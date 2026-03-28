
with customers as (
    select 
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    from {{ ref('stg__customers') }}
)
select * from customers