import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_latest_cutoff():

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        database="ecommerce",
        user=os.getenv('PG_USER'),
        password=os.getenv('PG_PASS'),
        host="postgres",
        port="5432"
    )

    # Create a cursor
    cur = conn.cursor()

    tables = ["customer_acquisition_channels",
              "customers",
              "inventory",
              "order_items",
              "orders",
              "product_categories",
              "products"]
    tbl_latest_cutoff = {}
    for table in tables:
        query = f"""
            SELECT latest_cutoff_at FROM meta_migration
            where table_name = '{table}'
        """
        cur.execute(query)
        data = cur.fetchone()

        # set latest cutoff timestamp for each table
        if data:
            cutoff_at = data[0]
        else:
            cutoff_at = '1900-01-01 00:00:00'
        tbl_latest_cutoff[table] = cutoff_at

    # Close postgres connection
    cur.close()
    conn.close()

    return tbl_latest_cutoff


def set_latest_cutoff(cur, table, latest_cutoff):
    pass

tables = ["customer_acquisition_channels",
            "customers",
            "inventory",
            "order_items",
            "orders",
            "product_categories",
            "products"]

# Define the DAG
with DAG(
    dag_id='test_extract_pg_to_bronze',
    start_date=days_ago(1),
    schedule_interval=None,  # Adjust the schedule as needed
    catchup=False,
    max_active_tasks=4, # Control parallelism
) as dag:
    
    _start = DummyOperator(task_id='dag_start')
    _end = DummyOperator(task_id='dag_end')
    
    with TaskGroup("extract_and_load_bronze", tooltip="Extract and Load Tables") as extract_and_load:
        for table in tables:
            extract_load = SSHOperator(
                task_id=f'el_pg_to_bronze_{table}',
                ssh_conn_id='ssh_spark',
                command=f"""export AWS_REGION={os.getenv('AWS_REGION')} \
                    export AWS_ACCESS_KEY_ID={os.getenv('AWS_ACCESS_KEY_ID')} \
                    export AWS_SECRET_ACCESS_KEY={os.getenv('AWS_SECRET_ACCESS_KEY')} \
                    export POSTGRES_TABLE={table} \
                    && /opt/spark/bin/spark-submit /home/spark/jobs/load_pgdata_to_datalake.py     
                """
            )

            extract_load


    # Set the task dependencies
    _start >> extract_and_load >> _end
