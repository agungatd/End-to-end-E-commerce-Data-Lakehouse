# End-to-end-E-commerce-Data-Lakehouse
A comprehensive data engineering project built on the medallion architecture, utilizing Iceberg for a scalable and reliable data lakehouse. Extracts data from diverse sources, transforms and loads it into the lakehouse, and ultimately feeds it into Metabase for insightful visualizations.
---
## Steps to run:
1. ```
    git clone https://github.com/tabular-io/docker-spark-iceberg.git
    cd docker-spark-iceberg
    ```
2. replace all value in `.temp.env` files with your own credentials and rename the file to `.env`
3. `bash start.sh`
4. now you can open on your browser these web clients (all user & password can be seen from .env files):
    - airflow Web UI: http://localhost:8081/
    - spark master UI: http://localhost:8080/
    - Jupyter Notebooks: http://localhost:8888/
    - MinIO console: http://localhost:9001/
    - Metabase: http://localhost:5000/
## Steps:
In medallion architecture ...
### 1. Data Extraction (Bronze)
#### 1.1. Postgres to MinIO (RDBMS -> Data Lake Storage)
We will be using airflow SshOperator. steps to do in order for airflow container to open ssh connection into spark container as follow:
- login to spark-iceberg container
`docker exec -it spark-iceberg bash`
- run these commands:
```bash
passwd # to create new password that will be used when open ssh connection.
vim /etc/ssh/sshd_config 
```
change the line `PermitRootLogin yes`
- restart ssh
`service ssh restart`
- now try to open ssh conn from airflow-scheduler container. outside the container, run:
`docker exec -it airflow-scheduler bash`
then run `ssh -o StrictHostKeyChecking=accept-new root@spark-iceberg`
when asked the password, insert the password you made before in the spark-iceberg container (the `passwd` command)
#### 1.2. MongoDB to MinIO (NoSQL -> Data Lake Storage)
#### 1.3. Unstructured Data to MinIO
### 2. Data Transformation (Silver)
### 3. Transformed Data Saves to Gold
### 4. Dashboard Building
## Notes:
- We will use postgres for both airflow metadata and for this project RDBMS source data.
- before run simple_etl_test, run this command: `docker cp data/test_dummy/customers.csv ecom-lakehouse-airflow-scheduler-1:/opt/airflow/customers.csv`

## Ref:
- https://github.com/databricks/docker-spark-iceberg/tree/main
- https://blog.min.io/a-developers-introduction-to-apache-iceberg-using-minio/
- https://blog.min.io/building-a-data-lakehouse-using-apache-iceberg-and-minio/
- https://github.com/anittasaju1996/MyProjects/blob/master/Airflow_SSHOperator_Spark_in_Docker/README.md
- https://blog.min.io/stream-data-to-minio-using-kafka-kubernetes/
- https://medium.com/@arnab.neogi.86/apache-iceberg-nessie-rest-catalog-minio-spark-trino-and-duckdb-part-2-6f0aee21e8d9
- https://towardsdev.com/%EF%B8%8Fusing-apache-iceberg-with-apache-spark-and-minio-docker-dee475b55a8f (https://github.com/abdelbarre/Spark-iceberg)
- https://medium.com/nagoya-foundation/simple-cdc-with-debezium-kafka-a27b28d8c3b8

## What's Next:
- [] Implement CDC
- [] Create Data Quality test
- [] Implement CI/CD
- [] Increase data volume and implement distributed computing
- [] Use cloud and minimize cost / use free tier as much as possible
