CREATE TABLE IF NOT EXISTS fact_orders_created (
    order_id VARCHAR NOT NULL,
    product_id VARCHAR,
    created_date_id VARCHAR,
    created_time TIMESTAMP,
    amount DECIMAL,
    total_price DECIMAL,
    processed_time TIMESTAMP,
    UNIQUE(order_id)
);