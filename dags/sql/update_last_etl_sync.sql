DELETE FROM telco_internal.control_table WHERE table_name =  'telco_external.landing_telco_customer_churn_data';
INSERT INTO telco_internal.control_table (table_name, last_load_timestamp)
VALUES ('telco_external.landing_telco_customer_churn_data', CURRENT_TIMESTAMP);