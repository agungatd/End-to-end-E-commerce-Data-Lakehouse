#!/usr/bin/env bash

echo "Initializing the Airflow database..."
airflow db init

echo "Starting $1..."
exec airflow "$1"