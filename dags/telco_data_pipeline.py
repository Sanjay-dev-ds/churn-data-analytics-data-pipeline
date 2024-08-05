from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task_group
from airflow.decorators import (
    dag,
    task,
)
from airflow.models import Variable
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from pandas import DataFrame
from io import StringIO 
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook 

schema_name = 'telco_schema'
table_name = 'telco_customr_churn_data'
s3_bucket = 'telco-raw-data-lake'
s3_prefix = 'telco_customer_churn_data'
glue_crawler_name = 'telco-data-crawler'
REGION = 'us-east-1'
STAGE_TABLE_NAME = 'telco_internal.staging_telco_customer_churn_data'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 12),
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}


@dag(default_args=default_args, schedule_interval='@daily', catchup=False)
def telco_etl_data_pipeline():
    @task
    def trigger_glue_crawler():
        return GlueCrawlerOperator(
            task_id='run_glue_crawler',
            config={'Name': glue_crawler_name},
            aws_conn_id='aws_default',
            wait_for_completion = True,
            region_name = REGION,
            poll_interval = 10
        ).execute(context={})

    @task
    def ingest_data_into_s3(df: DataFrame, s3_bucket: str, s3_key: str):
        hook = S3Hook(aws_conn_id='aws_default')
        buffer = StringIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)
        hook.load_string(buffer.getvalue(), key=s3_key, bucket_name=s3_bucket)
        print(f"DataFrame ingested into s3://{s3_bucket}/{s3_key}")


    @task
    def extract_data(record_modified=None):
        pg_hook = PostgresHook(postgres_conn_id='telco_postgres_conn_id')
        with pg_hook.get_conn() as conn:
            cursor = conn.cursor()

            if record_modified is None:
                query = f'SELECT * FROM {schema_name}.{table_name};'
                print(query)
                cursor.execute(query)
            else:
                query = f'SELECT * FROM {schema_name}.{table_name} WHERE record_modified > %s;'
                cursor.execute(query, (record_modified,))

            rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
            print("Number of records : ",len(df))
            cursor.close() 
            return df

    @task
    def get_last_etl_sync():
        record_modified = Variable.get('record_modified', default_var=None)
        record_modified = None
        return record_modified

    @task
    def create_relevant_schema_on_redshift():
        redshift_hook = RedshiftSQLHook(redshift_conn_id='redshift_default')
        with redshift_hook.get_conn() as conn:
            cursor = conn.cursor()
            sql_file_path = 'dags/sql/create_relevant_schema.sql'
            with open(sql_file_path, 'r') as file:
                query = file.read()
            for stmt in query.strip().split(';'):
                if stmt.strip():
                    print("Executing : ", stmt)
                    cursor.execute(stmt)
            cursor.close()

    @task
    def create_common_tables():
        redshift_hook = RedshiftSQLHook(redshift_conn_id='redshift_default')
        with redshift_hook.get_conn() as conn:
            cursor = conn.cursor()
            sql_file_path = 'dags/sql/create_common_table.sql'
            with open(sql_file_path, 'r') as file:
                query = file.read()
            for stmt in query.strip().split(';'):
                if stmt.strip():
                    print("Executing : ", stmt)
                    cursor.execute(stmt)
            cursor.close()

    @task
    def create_or_populate_staging_table():
        redshift_hook = RedshiftSQLHook(redshift_conn_id='redshift_default')
        with redshift_hook.get_conn() as conn:
            cursor = conn.cursor()
            sql_file_path = 'dags/sql/prepare_staging_table.sql'
            with open(sql_file_path, 'r') as file:
                query = file.read()
            for stmt in query.strip().split(';'):
                if stmt.strip():
                    print("Executing : ", stmt)
                    cursor.execute(stmt)
            cursor.close()
        update_last_sync_time(STAGE_TABLE_NAME)

    @task 
    def update_last_sync_time(table_name) :
        redshift_hook = RedshiftSQLHook(redshift_conn_id='redshift_default')
        with redshift_hook.get_conn() as conn:
            cursor = conn.cursor()
            sql_file_path = 'dags/sql/update_last_etl_sync.sql'
            with open(sql_file_path, 'r') as file:
                query = file.read()
                formatted_query = query.format(table_name=table_name)
            for stmt in formatted_query.strip().split(';'):
                if stmt.strip():
                    print("Executing : ", stmt)
                    cursor.execute(stmt)
            cursor.close()

    @task_group
    def sync_staging_layer():
        chain([create_relevant_schema_on_redshift(), create_common_tables()],create_or_populate_staging_table())


    @task_group
    def ingest_data_task_group(record_modified):
        df = extract_data(record_modified)
        current_timestamp =  datetime.now()
        s3_key = f'{s3_prefix}/{current_timestamp.strftime('%Y%m%d%H%M%S')}/{table_name}.csv'
        ingest_data_into_s3(df, s3_bucket, s3_key)
        Variable.set('record_modified', current_timestamp)

    
    
    @task
    def dim_location_sync():
        redshift_hook = RedshiftSQLHook(redshift_conn_id='redshift_default')
        with redshift_hook.get_conn() as conn:
            cursor = conn.cursor()
            sql_file_path = 'dags/sql/dim_location.sql'
            with open(sql_file_path, 'r') as file:
                query = file.read()
            for stmt in query.strip().split(';'):
                if stmt.strip():
                    print("Executing : ", stmt)
                    cursor.execute(stmt)
            cursor.close()

        update_last_sync_time('telco_internal.dim_location')

    @task
    def dim_service_sync():
        redshift_hook = RedshiftSQLHook(redshift_conn_id='redshift_default')
        with redshift_hook.get_conn() as conn:
            cursor = conn.cursor()
            sql_file_path = 'dags/sql/dim_service.sql'
            with open(sql_file_path, 'r') as file:
                query = file.read()
            for stmt in query.strip().split(';'):
                if stmt.strip():
                    print("Executing : ", stmt)
                    cursor.execute(stmt)
            cursor.close()

        update_last_sync_time('telco_internal.dim_service')

    
    @task
    def dim_customer_sync():
        redshift_hook = RedshiftSQLHook(redshift_conn_id='redshift_default')
        with redshift_hook.get_conn() as conn:
            cursor = conn.cursor()
            sql_file_path = 'dags/sql/dim_customer.sql'
            with open(sql_file_path, 'r') as file:
                query = file.read()
            for stmt in query.strip().split(';'):
                if stmt.strip():
                    print("Executing : ", stmt)
                    cursor.execute(stmt)
            cursor.close()

        update_last_sync_time('telco_internal.dim_customer')



    @task_group
    def dimension_table_generation():
        location_task = dim_location_sync()
        service_task = dim_service_sync()
        customer_task = dim_customer_sync()
        
        [location_task, service_task, customer_task]


    
    @task_group
    def sync_consumable_layer():
         dimension_table_generation()



    last_sync = get_last_etl_sync()
    # ingest = ingest_data_task_group(last_sync) 
    # glue_crawler = trigger_glue_crawler()
    landing_layer = sync_staging_layer()
    consumption_layer = sync_consumable_layer()


    chain(last_sync,landing_layer,consumption_layer)

dag_instance = telco_etl_data_pipeline()