CREATE TABLE  IF NOT EXISTS  telco_internal.control_table (
    table_name VARCHAR(255) PRIMARY KEY,
    last_load_timestamp TIMESTAMP
);