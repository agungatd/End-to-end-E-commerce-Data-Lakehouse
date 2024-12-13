import os
from datetime import datetime
from pyspark.sql import SparkSession


ENV = {
    'SPARK_APPNAME': os.getenv('SPARK_APPNAME', 'APPNAME HAS NOT BEEN SET'),
    'EXTRACT_QUERY': os.getenv('EXTRACT_QUERY', ''),
    'LOAD_QUERY': os.getenv('LOAD_QUERY', ''),
    'JDBC_URL': os.getenv('JDBC_URL', ''),
    'ICEBERG_DB_TABLE': os.getenv('ICEBERG_DB_TABLE')
}

def get_df_postgres(spark, jdbc_url, query):
    df = spark.read.format('jdbc') \
        .option("url", jdbc_url) \
        .option("query",query) \
        .option("user","postgres") \
        .option("password","postgres") \
        .option("driver","org.postgresql.Driver") \
        .load()

    return df

def get_spark_df(spark):
    df = spark.table(ENV['ICEBERG_DB_TABLE'])
    return df

def get_dataframe(spark, query):
    jdbc_url = ENV['JDBC_URL']
    if 'postgres' in jdbc_url:
        df = get_df_postgres(spark, jdbc_url, query)
    else:
        df = get_spark_df(spark)
    
    return df

def transform_dataframe(df):
    # implement spark transformations
    return df

def load_dataframe(df, iceberg_table):
    # Write data to MinIO in Iceberg format
    # iceberg_table = f"demo.{schema}.{table}"
    df.writeTo(iceberg_table) \
        .append()

def etl(spark):
    EXTRACT_QUERY = ENV['EXTRACT_QUERY']
    ICEBERG_DB_TABLE = ENV['ICEBERG_DB_TABLE']

    print(f"Extracting Data")
    df = get_dataframe(spark, EXTRACT_QUERY)

    print("Transforming Data")
    df = transform_dataframe(df)

    print(f"Writing data to Iceberg table: {ICEBERG_DB_TABLE}")
    load_dataframe(df, ICEBERG_DB_TABLE)

    print(f"Data Extraction has completed successfully!")


if __name__=="__main__":
    appname = ENV['SPARK_APPNAME']

    # Initialize Spark session with necessary Iceberg and Hadoop configurations
    spark = SparkSession.builder \
        .appName(appname) \
        .getOrCreate()

    etl(spark)

    # Stop spark session
    spark.stop()
