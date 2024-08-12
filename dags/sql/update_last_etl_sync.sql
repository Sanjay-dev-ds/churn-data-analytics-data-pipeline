DELETE FROM telco_internal.control_table WHERE table_name =  '{table_name}';
INSERT INTO telco_internal.control_table (table_name, last_load_timestamp)
VALUES ('{table_name}', CURRENT_TIMESTAMP);