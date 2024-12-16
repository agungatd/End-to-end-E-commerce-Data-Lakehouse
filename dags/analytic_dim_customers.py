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
    dag_id='analytic_dim_customers',
    start_date=days_ago(1),
    schedule=None,  # Adjust the schedule as needed
    catchup=False,
) as dag:
    
    _start = DummyOperator(task_id='dag_start')
    _end = DummyOperator(task_id='dag_end')

    target_table = f"demo.dev_star_ecommerce.dim_customers"
    extract_query = f"""
    select * from dev_curated_ecommerce.customers c
    left join dev_raw_ecommerce.customer_acquisition_channels ac
        on c.acquisition_channel_id = ac.channel_id
    """
    spark_job = 'analytic_dim_customers.py'
    transform_load_task = SSHOperator(
        task_id=f'transform_customers',
        ssh_conn_id=SSH_CONN_ID,
        command=f"""{DEFAULT_SPARK_ENV} \
            export SPARK_APPNAME='{f"Silver to Gold: Dimension Customers"}' \
            export TARGET_TABLE={target_table} \
            export EXTRACT_QUERY='{extract_query}' \
            export INSERT_METHOD=overwrite \
            export PARTITION_BY=registration_date \
            && /opt/spark/bin/spark-submit /home/spark/jobs/{spark_job}     
        """
    )

    # Set the task dependencies
    _start >> transform_load_task >> _end
