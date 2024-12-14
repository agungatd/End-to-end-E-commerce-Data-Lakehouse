import os

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

from dotenv import load_dotenv

load_dotenv()

DEFAULT_SPARK_ENV = f"""
    export AWS_REGION={os.getenv('AWS_REGION')} \
    export AWS_ACCESS_KEY_ID={os.getenv('AWS_ACCESS_KEY_ID')} \
    export AWS_SECRET_ACCESS_KEY={os.getenv('AWS_SECRET_ACCESS_KEY')}
"""
SSH_CONN_ID = 'ssh_spark'

# Define the DAG
with DAG(
    dag_id='curated_customers',
    start_date=days_ago(1),
    schedule=None,  # Adjust the schedule as needed
    catchup=False,
) as dag:
    
    _start = DummyOperator(task_id='dag_start')
    _end = DummyOperator(task_id='dag_end')

    source_table = f"demo.dev_raw_ecommerce.customers"
    target_table = f"demo.dev_curated_ecommerce.customers"
    extract_query = f'SELECT * FROM {source_table}'
    spark_job = 'curated_customers.py'
    transform_load_task = SSHOperator(
        task_id=f'transform_customers',
        ssh_conn_id=SSH_CONN_ID,
        command=f"""{DEFAULT_SPARK_ENV} \
            export SPARK_APPNAME='{f"Template for Extracting table to table: {source_table} -> {target_table}"}' \
            export SOURCE_TABLE={source_table} \
            export TARGET_TABLE={target_table} \
            export EXTRACT_QUERY='{extract_query}' \
            export PARTITION_BY=created_at \
            && /opt/spark/bin/spark-submit /home/spark/jobs/{spark_job}     
        """
    )

    # Set the task dependencies
    _start >> transform_load_task >> _end
