import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

import polars as pl
from dotenv import load_dotenv

load_dotenv()

def get_latest_cutoff(cur, table):
    # Create a cursor
    query = f"""
        SELECT latest_cutoff_at FROM meta_migration
        where table_name = '{table}'
    """
    cur.execute(query)
    data = cur.fetchall()

    return data[0][0]

def set_latest_cutoff(cur, table, latest_cutoff):
    pass


# Define the DAG
with DAG(
    dag_id='test_extract_pg_to_bronze',
    start_date=days_ago(1),
    schedule_interval=None,  # Adjust the schedule as needed
    catchup=False
) as dag:
    
    _start = DummyOperator(task_id='dag_start')
    _end = DummyOperator(task_id='dag_end')

    process_customers = SSHOperator(
        task_id='load_postgres_to_bronze',
        ssh_conn_id='ssh_spark',
        command=f"""export AWS_REGION={os.getenv('AWS_REGION')} \
            && export AWS_ACCESS_KEY_ID={os.getenv('AWS_ACCESS_KEY_ID')} \
            && export AWS_SECRET_ACCESS_KEY={os.getenv('AWS_SECRET_ACCESS_KEY')} \
            && /opt/spark/bin/spark-submit /home/spark/jobs/load_pgdata_to_datalake.py     
        """
    )

    # Set the task dependencies
    _start >> process_customers >> _end