CREATE TABLE IF NOT EXISTS telco_internal.staging_telco_customer_churn_data (
    customerid character varying(16383) ENCODE lzo,
    count bigint ENCODE az64,
    country character varying(16383) ENCODE lzo,
    state character varying(16383) ENCODE lzo,
    city character varying(16383) ENCODE lzo,
    zip_code bigint ENCODE az64,
    latitude double precision ENCODE raw,
    longitude double precision ENCODE raw,
    gender character varying(16383) ENCODE lzo,
    seniorcitizen character varying(16383) ENCODE lzo,
    partner character varying(16383) ENCODE lzo,
    dependents character varying(16383) ENCODE lzo,
    tenure bigint ENCODE az64,
    phoneservice character varying(16383) ENCODE lzo,
    multiplelines character varying(16383) ENCODE lzo,
    internetservice character varying(16383) ENCODE lzo,
    onlinesecurity character varying(16383) ENCODE lzo,
    onlinebackup character varying(16383) ENCODE lzo,
    deviceprotection character varying(16383) ENCODE lzo,
    techsupport character varying(16383) ENCODE lzo,
    streamingtv character varying(16383) ENCODE lzo,
    streamingmovies character varying(16383) ENCODE lzo,
    contract character varying(16383) ENCODE lzo,
    paperlessbilling character varying(16383) ENCODE lzo,
    paymentmethod character varying(16383) ENCODE lzo,
    monthlycharges double precision ENCODE raw,
    totalcharges double precision ENCODE raw,
    churn_label character varying(16383) ENCODE lzo,
    churn_value bigint ENCODE az64,
    churn_score bigint ENCODE az64,
    cltv bigint ENCODE az64,
    churn_reason character varying(16383) ENCODE lzo,
    record_modified character varying(16383) ENCODE lzo
) DISTSTYLE AUTO;



INSERT INTO telco_internal.staging_telco_customer_churn_data (
    CustomerID, Count, Country, State, City, Zip_Code,
    Latitude, Longitude, Gender, SeniorCitizen, Partner, Dependents,
    Tenure, PhoneService, MultipleLines, InternetService, OnlineSecurity,
    OnlineBackup, DeviceProtection, TechSupport, StreamingTV, StreamingMovies,
    Contract, PaperlessBilling, PaymentMethod, MonthlyCharges, TotalCharges,
    Churn_Label, Churn_Value, Churn_Score, CLTV, Churn_Reason, Record_Modified
)
WITH last_load AS (
    SELECT last_load_timestamp
    FROM telco_internal.control_table
    WHERE table_name = 'telco_internal.staging_telco_customer_churn_data'
)
SELECT
    DISTINCT CustomerID, Count, Country, State, City, "zip code",
    Latitude, Longitude, Gender, "senior citizen", Partner, Dependents,
    "tenure months", "phone service", "multiple lines", "internet service", "online security",
    "online backup", "device protection", "tech support", "streaming tv", "streaming movies",
    Contract, "paperless billing", "payment method", "monthly charges", "total charges",
    "churn label", "churn value", "churn score", "cltv", "churn reason", "record_modified"
FROM telco_external.landing_telco_customer_churn_data
WHERE "record_modified" > (SELECT COALESCE(MAX(last_load_timestamp), '1900-01-01') FROM last_load);