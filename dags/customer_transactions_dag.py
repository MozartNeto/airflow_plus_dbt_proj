from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
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

def load_csv_from_url(**context):
    url = 'https://raw.githubusercontent.com/MozartNeto/airflow_plus_dbt_proj/refs/heads/main/data_file/customer_transactions.csv'
    
    print(f"Downloading file from: {url}")
    response = requests.get(url)
    response.raise_for_status()
    
    csv_data = StringIO(response.text)
    df = pd.read_csv(csv_data)
    df = df.astype(str)
    
    print(f"Loaded {len(df)} rows from CSV")
    hook = PostgresHook(postgres_conn_id='postgres_dw')
    conn = hook.get_conn()
    cursor = conn.cursor()
    columns = df.columns.tolist()
    
    column_defs = ', '.join([f'"{col}" TEXT' for col in columns])
    create_table_sql = f"""
    DROP TABLE IF EXISTS raw_customer_transactions;
    CREATE TABLE raw_customer_transactions (
        {column_defs},
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    cursor.execute(create_table_sql)

    placeholders = ', '.join(['%s'] * len(columns))
    insert_sql = f"""
    INSERT INTO raw_customer_transactions ({', '.join([f'"{col}"' for col in columns])})
    VALUES ({placeholders})
    """
    
    for _, row in df.iterrows():
        cursor.execute(insert_sql, tuple(row))
    
    conn.commit()
    cursor.close()
    conn.close()

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
