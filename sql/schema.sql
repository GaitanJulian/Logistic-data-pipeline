-- Schema definitions for logistics pipeline
CREATE TABLE IF NOT EXISTS dim_city (
    city_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id INT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS fact_shipment (
    shipment_id INT PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES dim_customer(customer_id),
    origin_city_id INT NOT NULL REFERENCES dim_city(city_id),
    destination_city_id INT NOT NULL REFERENCES dim_city(city_id),
    created_at TIMESTAMP NOT NULL,
    delivered_at TIMESTAMP NULL,
    status VARCHAR(50) NOT NULL,
    weight_kg NUMERIC(10,2) NOT NULL,
    price NUMERIC(12,2) NOT NULL,
    delivery_time_hours NUMERIC(10,2),
    is_delayed BOOLEAN
);

CREATE TABLE IF NOT EXISTS etl_log (
    id SERIAL PRIMARY KEY,
    run_at TIMESTAMP NOT NULL DEFAULT NOW(),
    source_file VARCHAR(255),
    rows_read INT,
    rows_loaded INT,
    rows_error INT
);
