CREATE TABLE IF NOT EXISTS dim_products (
    id VARCHAR NOT NULL,
    title VARCHAR,
    category VARCHAR,
    price DECIMAL,
    processed_time TIMESTAMP,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
UNIQUE(id, start_time)
);
