
with orders as (
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
dim_orders as (
    select 
        o.order_id,
        o.order_status,
        o.order_purchase_date,
        o.order_approved_date,
        o.order_delivered_carrier_date as order_delivered_carrier_date,
        o.order_delivered_customer_date as order_delivered_customer_date,
        o.order_estimated_delivery_date as order_estimated_delivery_date,
        op.payment_method
    from orders o
    left join order_payments op on o.order_id = op.order_id
)
select * from dim_orders