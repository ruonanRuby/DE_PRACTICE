INSERT INTO fact_orders_created(order_id, product_id, created_time, created_date_id, amount, total_price, processed_time)
SELECT stg_orders.id AS order_id,
    product_id,
    event_time as created_time,
    dim_dates.id as created_date_id,
    amount,
    total_price,
    '{{ ts }}'
FROM stg_orders
INNER JOIN dim_dates on dim_dates.datum = date(event_time)
ON CONFLICT(order_id) DO NOTHING