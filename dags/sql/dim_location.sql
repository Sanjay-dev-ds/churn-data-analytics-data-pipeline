CREATE TABLE IF NOT EXISTS telco_internal.dim_location (
    location_key INT IDENTITY(1,1) PRIMARY KEY,
    Country VARCHAR(255),
    State VARCHAR(255),
    City VARCHAR(255),
    Zip_Code INT,
    Latitude FLOAT,
    Longitude FLOAT
);


INSERT INTO telco_internal.dim_location (Country, State, City, Zip_Code, Latitude, Longitude)
WITH last_load AS (
    SELECT last_load_timestamp
    FROM telco_internal.control_table
    WHERE table_name = 'telco_internal.dim_location'
)
SELECT DISTINCT Country, State, City, Zip_Code, Latitude, Longitude
FROM telco_internal.staging_telco_customer_churn_data
WHERE "record_modified" > (SELECT COALESCE(MAX(last_load_timestamp), '1900-01-01') FROM last_load);



DELETE FROM telco_internal.control_table WHERE table_name =  'telco_internal.dim_location';
INSERT INTO telco_internal.control_table (table_name, last_load_timestamp)
VALUES ('telco_internal.dim_location', CURRENT_TIMESTAMP);