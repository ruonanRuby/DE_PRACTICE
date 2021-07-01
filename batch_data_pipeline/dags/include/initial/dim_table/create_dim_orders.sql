CREATE TABLE IF NOT EXISTS dim_orders (
    order_id VARCHAR NOT NULL,
    status VARCHAR,
    event_time timestamp,
    processed_time timestamp,
    start_time timestamp,
    end_time timestamp,
    UNIQUE(order_id, start_time)
);