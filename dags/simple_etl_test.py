from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

import polars as pl
import psycopg2

# Function to extract, transform, and load the data
def etl_data(file_path, table_name):
# Read the CSV file using Polars
    df = pl.read_csv(file_path)
    print(f'Source Data: {df}')

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        database="ecommerce",
        user="postgres",
        password="postgres",
        host="postgres",
        port="5432"
    )

    # Create a cursor
    cur = conn.cursor()

    # Insert the data into the table
    for row in df.iter_rows():
        cur.execute(f"INSERT INTO {table_name} VALUES ({', '.join(['%s'] * len(row))})", row)

    # Commit the changes and close the connection
    conn.commit()
    cur.close()
    conn.close()

# Define the DAG
with DAG(
    'test_simple_etl',
    start_date=days_ago(1),
    schedule_interval=None,  # Adjust the schedule as needed
    catchup=False
) as dag:
    
    _start = DummyOperator(task_id='dag_start')
    _end = DummyOperator(task_id='dag_end')

    # Task to process the customers data
    process_customers = PythonOperator(
        task_id='process_customers',
        python_callable=etl_data,
        op_kwargs={'file_path': '/opt/airflow/customers.csv', 'table_name': 'customers'}
    )

    # Set the task dependencies
    _start >> process_customers >> _end