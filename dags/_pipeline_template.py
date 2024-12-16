import os

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

from dotenv import load_dotenv

load_dotenv()

ENV = {
    'pg_user': os.getenv('PG_USER'),
    'pg_pass': os.getenv('PG_PASS'),
}
DEFAULT_SPARK_ENV = f"""
    export AWS_REGION={os.getenv('AWS_REGION')} \
    export AWS_ACCESS_KEY_ID={os.getenv('AWS_ACCESS_KEY_ID')} \
    export AWS_SECRET_ACCESS_KEY={os.getenv('AWS_SECRET_ACCESS_KEY')}
"""
SSH_CONN_ID = 'ssh_spark'

# Define the DAG
with DAG(
    dag_id='_pipeline_template',
    start_date=days_ago(1),
    schedule=None,  # Adjust the schedule as needed
    catchup=False,
) as dag:
    
    _start = DummyOperator(task_id='dag_start')
    _end = DummyOperator(task_id='dag_end')
    
    source_schema = 'DEV'
    target_schema = 'dev_temp_raw'
    source_table = '_temp_table'
    target_table = '_temp_table'
    extract_query = f'SELECT * FROM {source_table}'
    iceberg_table = f"demo.{target_schema}.{target_table}"
    spark_job = '_temp_extract.py'
    extract_load_task = SSHOperator(
        task_id=f'extract_{source_table}',
        ssh_conn_id=SSH_CONN_ID,
        command=f"""{DEFAULT_SPARK_ENV} \
            export SPARK_APPNAME='{f"Template for Extracting table to table: {source_table} -> {target_table}"}' \
            export SOURCE_TABLE={source_table} \
            export TARGET_TABLE={target_table} \
            export EXTRACT_QUERY='{extract_query}' \
            export JDBC_URL={f"jdbc:postgresql://postgres:5432/{source_schema}?user={ENV['pg_user']}&password={ENV['pg_pass']}"} \
            export ICEBERG_DB_TABLE={iceberg_table} \
            && /opt/spark/bin/spark-submit /home/spark/jobs/{spark_job}     
        """
    )

    source_schema = 'dev_temp_raw'
    target_schema = 'dev_temp_curated'
    source_table = '_temp_table'
    target_table = '_temp_table'
    extract_query = f'SELECT * FROM {source_table}'
    iceberg_table = f"demo.{target_schema}.{target_table}"
    spark_job = '_temp_transform.py'
    transform_load_task = SSHOperator(
        task_id=f'transform_{source_table}',
        ssh_conn_id=SSH_CONN_ID,
        command=f"""{DEFAULT_SPARK_ENV} \
            export SPARK_APPNAME='{f"Template for Extracting table to table: {source_table} -> {target_table}"}' \
            export SOURCE_TABLE={source_table} \
            export TARGET_TABLE={target_table} \
            export EXTRACT_QUERY='{extract_query}' \
            export ICEBERG_DB_TABLE={iceberg_table} \
            && /opt/spark/bin/spark-submit /home/spark/jobs/{spark_job}     
        """
    )

    # Set the task dependencies
    _start >> extract_load_task >> transform_load_task >> _end
