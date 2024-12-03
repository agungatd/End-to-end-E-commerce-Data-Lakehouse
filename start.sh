#!/bin/bash

echo "Hello World!"

echo "remove running containers"
docker compose down

echo "run all services from docker compose"
docker compose up -d

echo "Copy postgres jar, for migrating data from postgres to data lakehouse"
docker cp ./spark/jars/postgresql-42.7.4.jar spark-iceberg:/opt/spark/jars/

echo "Run initial tables creation using iceberg"
docker exec spark-iceberg mkdir /opt/spark/sql
docker cp ./spark/sql/* spark-iceberg:/opt/spark/sql/
docker exec spark-iceberg spark-sql -f sql/create_tables.sql

echo "Copy spark scripts"
docker exec spark-iceberg mkdir /opt/spark/jobs
docker cp ./spark/scripts/* spark-iceberg:/opt/spark/jobs/
docker exec spark-iceberg spark-submit jobs/migrate_pg_datalake.py