import os
from datetime import datetime
from pyspark.sql import SparkSession


ENV = {
    'SPARK_APPNAME': os.getenv('SPARK_APPNAME', 'APPNAME HAS NOT BEEN SET'),
    'EXTRACT_QUERY': os.getenv('EXTRACT_QUERY', ''),
    'LOAD_QUERY': os.getenv('LOAD_QUERY', ''),
    'JDBC_URL': os.getenv('JDBC_URL', ''),
    'ICEBERG_DB_TABLE': os.getenv('ICEBERG_DB_TABLE'),
    'UPSERT_QUERY': os.getenv('UPSERT_QUERY'),
}

def get_df_postgres(spark, jdbc_url, query):
    df = spark.read.format('jdbc') \
        .option("url", jdbc_url) \
        .option("query",query) \
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

def upsert_dataframe(spark, query):
    spark.sql(query)
        
def load_dataframe(df, iceberg_table, method='append', **kwargs):
    # Write data to MinIO in Iceberg format
    try:
        if method == 'overwrite':
            partition_by = kwargs['partition_by']
            if not partition_by:
                raise Exception('overwrite method need partition_by column!')
            df.writeTo(iceberg_table).partitionedBy(partition_by).createOrReplace()

        else:
            df.writeTo(iceberg_table).append()
    except Exception as e:
        print(f'load_dataframe Error: {e}')

def etl(spark):
    print(f"Extracting Data")
    df = get_dataframe(spark, ENV['EXTRACT_QUERY'])

    print("Transforming Data")
    df = transform_dataframe(df)

    print(f"Writing data to Iceberg table")
    # load_dataframe(df, ENV['ICEBERG_DB_TABLE'])
    upsert_dataframe(spark, ENV['UPSERT_QUERY'])

    print(f"Data Extraction has completed successfully!")


if __name__=="__main__":
    # Initialize Spark session with necessary Iceberg and Hadoop configurations
    spark = SparkSession.builder \
        .appName(ENV['SPARK_APPNAME']) \
        .getOrCreate()

    etl(spark)

    # Stop spark session
    spark.stop()
