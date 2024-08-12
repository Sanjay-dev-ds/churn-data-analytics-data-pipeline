CREATE TABLE IF NOT EXISTS telco_internal.dim_customer (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID VARCHAR(255),
    Gender VARCHAR(50),
    SeniorCitizen VARCHAR(50),
    Partner VARCHAR(50),
    Dependents VARCHAR(50),
    EffectiveDate DATE DEFAULT CURRENT_DATE,
    EndDate DATE DEFAULT '9999-12-31',
    IsCurrent BOOLEAN DEFAULT TRUE
);


INSERT INTO telco_internal.dim_customer (CustomerID, Gender, SeniorCitizen, Partner, Dependents)
WITH last_load AS (
    SELECT last_load_timestamp
    FROM telco_internal.control_table
    WHERE table_name = 'telco_internal.dim_service'
)
SELECT DISTINCT CustomerID, Gender, SeniorCitizen, Partner, Dependents
FROM telco_internal.staging_telco_customer_churn_data
WHERE "record_modified" > (SELECT COALESCE(MAX(last_load_timestamp), '1900-01-01') FROM last_load);

-- SCD TYPE 2 Implementation

-- Add a new version of a customer record if there's a change
INSERT INTO telco_internal.dim_customer (CustomerID, Gender, SeniorCitizen, Partner, Dependents, EffectiveDate, IsCurrent)
SELECT
    sc.CustomerID,
    sc.Gender,
    sc.SeniorCitizen,
    sc.Partner,
    sc.Dependents,
    CURRENT_DATE,
    TRUE
FROM telco_internal.staging_telco_customer_churn_data sc
LEFT JOIN telco_internal.dim_customer dc ON sc.CustomerID = dc.CustomerID
WHERE dc.IsCurrent = TRUE
  AND (sc.Gender <> dc.Gender OR sc.SeniorCitizen <> dc.SeniorCitizen OR sc.Partner <> dc.Partner OR sc.Dependents <> dc.Dependents);


-- Mark the old version as not current
UPDATE telco_internal.dim_customer
SET EndDate = CURRENT_DATE,
    IsCurrent = FALSE
WHERE CustomerID IN (
    SELECT sc.CustomerID
    FROM telco_internal.staging_telco_customer_churn_data sc
    LEFT JOIN telco_internal.dim_customer dc ON sc.CustomerID = dc.CustomerID
    WHERE dc.IsCurrent = TRUE
      AND (sc.Gender <> dc.Gender OR sc.SeniorCitizen <> dc.SeniorCitizen OR sc.Partner <> dc.Partner OR sc.Dependents <> dc.Dependents)
);