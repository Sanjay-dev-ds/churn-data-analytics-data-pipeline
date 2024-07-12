from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.decorators import task_group
from airflow.operators.bash_operator import BashOperator
import pandas as pd
from airflow.decorators import (
    dag,
    task,
)
from airflow.models import Variable

db_name = 'telco'
db_user = 'telco'
db_password = 'Telco$123'
db_host = 'telco.c3e6o080ulrl.us-east-1.rds.amazonaws.com'
table_name = 'telco_customer_churn_data'
s3_bucket = 'your-s3-bucket'
s3_prefix = 'telco_customer_churn_data'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='telco_etl_pipeline',
    schedule_interval="5 * * * * *",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["example"],
) as dag:

    @task
    def extract_data(record_modified=None):
        pg_hook = PostgresHook(postgres_conn_id='postgres_conn_id')
        with pg_hook.get_conn() as conn:
            cursor = conn.cursor()

            if record_modified is None:
                query = f'SELECT * FROM {table_name};'
            else:
                query = f'SELECT * FROM {table_name} WHERE record_modified > %s;'
                cursor.execute(query, (record_modified,))

            rows = cursor.fetchall()
            return pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])

    @task
    def get_last_etl_sync():
        record_modified = Variable.get('record_modified', default_var=None)
        return record_modified
    

    @task()
    def ingest_data_into_s3(df, s3_bucket, s3_key):
        csv_data = df.to_csv(index=False)
        task = BashOperator(
        task_id='ingest_data_into_s3',  
        bash_command=f'echo "{csv_data}" | aws s3 cp - s3://{s3_bucket}/{s3_key}',
        dag=dag,
        )

    
    @task_group
    def ingest_data_task_group(record_modified):
        df = extract_data(record_modified)
        current_timestamp =  datetime.now().strftime('%Y%m%d%H%M%S')
        s3_key = f'{s3_prefix}/{current_timestamp}/'
        ingest_data_into_s3(df, s3_bucket, s3_key)
        Variable.set('record_modified', current_timestamp)

    check_last_etl_sync = get_last_etl_sync()
    ingest_data = ingest_data_task_group(check_last_etl_sync)

    check_last_etl_sync >> ingest_data