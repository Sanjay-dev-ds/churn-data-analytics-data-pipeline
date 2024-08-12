CREATE TABLE IF NOT EXISTS telco_internal.dim_service (
    service_key INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID VARCHAR(255),
    PhoneService VARCHAR(50),
    MultipleLines VARCHAR(50),
    InternetService VARCHAR(50),
    OnlineSecurity VARCHAR(50),
    OnlineBackup VARCHAR(50),
    DeviceProtection VARCHAR(50),
    TechSupport VARCHAR(50),
    StreamingTV VARCHAR(50),
    StreamingMovies VARCHAR(50)
);


INSERT INTO telco_internal.dim_service (CustomerID, PhoneService, MultipleLines, InternetService, OnlineSecurity, OnlineBackup, DeviceProtection, TechSupport, StreamingTV, StreamingMovies)
WITH last_load AS (
    SELECT last_load_timestamp
    FROM telco_internal.control_table
    WHERE table_name = 'telco_internal.dim_service'
)
SELECT DISTINCT CustomerID, PhoneService, MultipleLines, InternetService, OnlineSecurity, OnlineBackup, DeviceProtection, TechSupport, StreamingTV, StreamingMovies
FROM telco_internal.staging_telco_customer_churn_data
WHERE "record_modified" > (SELECT COALESCE(MAX(last_load_timestamp), '1900-01-01') FROM last_load);