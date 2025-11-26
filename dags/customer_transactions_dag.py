from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql
from datetime import datetime
import pandas as pd
import requests
from io import StringIO

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 11, 25),
    'retries': 2,
}

dag = DAG(
    'load_customer_transactions',
    default_args=default_args,
    description='Load customer transactions from Azure Blob to Postgres, and create a dimensional model with DBT',
    catchup=False,
    tags=['ingestion', 'dbt', 'customer_transaction_model'],
)

def get_csv_content_from_url(url):
    print(f"Downloading file from: {url}")
    response = requests.get(url)
    response.raise_for_status()
    
    return StringIO(response.text)

def csv_buffer_to_dataframe(csv_buffer):
    df = pd.read_csv(csv_buffer)
    df = df.astype(str)
    df['processed_at'] = pd.Timestamp.now()

    return df

def get_postgres_connection_uri():
    hook = PostgresHook(postgres_conn_id='postgres_dw')
    return hook.get_uri()

def load_pandas_df_to_pg(df, pg_conn_url, table_name, schema, if_exists = "replace"):
    df.to_sql(
        name=table_name,
        con=pg_conn_url, 
        schema=schema,
        if_exists=if_exists,
        index=False,
        method='multi', 
        chunksize=10000 
    )

def load_csv_from_url(**context):
    url = 'https://raw.githubusercontent.com/MozartNeto/airflow_plus_dbt_proj/refs/heads/main/data_file/customer_transactions.csv'
     
    csv_buffer = get_csv_content_from_url(url)
    df = csv_buffer_to_dataframe(csv_buffer)
    conn_uri = get_postgres_connection_uri()
    load_pandas_df_to_pg(df, conn_uri, "raw_customer_transactions", "public")


load_data_task = PythonOperator(
    task_id='load_data_from_url',
    python_callable=load_csv_from_url,
    dag=dag,
)

run_dbt_model = BashOperator(
    task_id='run_dbt_model',
    bash_command='docker exec airflow_plus_dbt_proj-dbt-1 dbt run',
    dag=dag,
)

load_data_task >> run_dbt_model
