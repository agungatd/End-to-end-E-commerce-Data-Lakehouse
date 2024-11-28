# End-to-end-E-commerce-Data-Lakehouse
A comprehensive data engineering project built on the medallion architecture, utilizing Iceberg for a scalable and reliable data lakehouse. Extracts data from diverse sources, transforms and loads it into the lakehouse, and ultimately feeds it into Metabase for insightful visualizations.
---

Notes:
- We will use postgres for both airflow metadata and for this project RDBMS source data.
- before run simple_etl_test, run this command: `docker cp data/test_dummy/customers.csv ecom-lakehouse-airflow-scheduler-1:/opt/airflow/customers.csv`
