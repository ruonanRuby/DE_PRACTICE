CREATE TABLE IF NOT EXISTS stg_orders (
    id VARCHAR NOT NULL,
    product_id VARCHAR,
    amount DECIMAL,
    total_price DECIMAL,
    status VARCHAR,
    event_time TIMESTAMP,
    processed_time TIMESTAMP
);

TRUNCATE stg_orders;