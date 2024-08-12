CREATE TABLE IF NOT EXISTS telco_internal.fact_churn (
    churn_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_key INT,
    location_key INT,
    service_key INT,
    Churn_Label VARCHAR(50),
    Churn_Value INT,
    Churn_Score INT,
    CLTV INT,
    Churn_Reason VARCHAR(255),
    FOREIGN KEY (customer_key) REFERENCES telco_internal.dim_customer(customer_key),
    FOREIGN KEY (location_key) REFERENCES telco_internal.dim_location(location_key),
    FOREIGN KEY (service_key) REFERENCES telco_internal.dim_service(service_key)
);


INSERT INTO telco_internal.fact_churn (
    Churn_Label,
    Churn_Value,
    Churn_Score,
    CLTV,
    Churn_Reason,
    customer_key,
    location_key,
    service_key
)
WITH last_load AS (
    SELECT last_load_timestamp
    FROM telco_internal.control_table
    WHERE table_name = 'telco_internal.fact_churn'
)
SELECT
    sc.Churn_Label,
    sc.Churn_Value,
    sc.Churn_Score,
    sc.CLTV,
    sc.Churn_Reason,
    dc.customer_key,
    dl.location_key,
    ds.service_key
FROM telco_internal.staging_telco_customer_churn_data sc
JOIN telco_internal.dim_customer dc ON sc.CustomerID = dc.CustomerID AND dc.IsCurrent = TRUE
JOIN telco_internal.dim_location dl ON sc.Zip_Code = dl.Zip_Code
JOIN telco_internal.dim_service ds ON sc.CustomerID = ds.CustomerID
WHERE  "record_modified" >  (SELECT COALESCE(MAX(last_load_timestamp), '1900-01-01') FROM last_load);