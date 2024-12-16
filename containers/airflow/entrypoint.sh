#!/usr/bin/env bash

echo "Initializing the Airflow database..."
airflow db migrate

echo "Creating admin user..."
airflow users create \
    --username $AIRFLOW_ADMIN_USER \
    --firstname $AIRFLOW_ADMIN_FIRST_NAME \
    --lastname $AIRFLOW_ADMIN_LAST_NAME \
    --role Admin \
    --email $AIRFLOW_ADMIN_EMAIL \
    --password $AIRFLOW_ADMIN_PASS

airflow connections add \
    --conn-host spark-iceberg
    --conn-login root
    --conn-password $SPARK_ICEBERG_PASSWD
    --conn-type ssh
    ssh_spark
    
echo "Starting $1..."
exec airflow "$1"