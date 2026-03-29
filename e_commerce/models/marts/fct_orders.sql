with dim_customers as (
    select
        customer_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    from {{ ref('stg__customers') }}
),
dim_orders as (
    select 
        order_id,
        customer_id,
        order_status,
        order_purchase_date,
        order_approved_date,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date
    from {{ ref('stg__orders') }}
),
order_payments as (
    select 
        order_id,
        payment_method,
        payment_installments,
        payment_value
    from {{ ref('stg__order_payments') }}
),
fct_orders as (
    select 
        o.order_id,
        o.customer_id,
        op.payment_method,
        op.payment_installments,
        op.payment_value
    from dim_customers c
    left join dim_orders o on o.customer_id = c.customer_id
    left join order_payments op on o.order_id = op.order_id
)
select * from fct_orders