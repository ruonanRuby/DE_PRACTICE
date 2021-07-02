CREATE TABLE IF NOT EXISTS fact_inventory_snapshot(
    product_id VARCHAR NOT NULL,
    amount DECIMAL,
    date_id    VARCHAR,
    processed_time TIMESTAMP,
    UNIQUE(product_id)
);

TRUNCATE fact_inventory_snapshot;
