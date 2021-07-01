CREATE TABLE IF NOT EXISTS stg_products (
    id VARCHAR NOT NULL UNIQUE, 
    title VARCHAR,
    category VARCHAR,
    price DECIMAL,
    processed_time TIMESTAMP
);

truncate stg_products;