with payments as (

    select order_id,
            sum(amount) as amount

    from {{ ref('stg_payments') }}

    group by order_id    

),

orders as (

    select * from {{ ref('stg_orders') }}

),


final as (

    select
        orders.*,
        coalesce(payments.amount, 0) as order_amount

    from orders

    left join payments using (order_id)
)

select * from final