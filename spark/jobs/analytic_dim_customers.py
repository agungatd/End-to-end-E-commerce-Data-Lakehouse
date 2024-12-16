import os
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, Row, StructType, StructField


ENV = {
    'SPARK_APPNAME': os.getenv('SPARK_APPNAME', 'APPNAME HAS NOT BEEN SET'),
    'EXTRACT_QUERY': os.getenv('EXTRACT_QUERY', ''),
    'LOAD_QUERY': os.getenv('LOAD_QUERY', ''),
    'JDBC_URL': os.getenv('JDBC_URL', ''),
    'SOURCE_TABLE': os.getenv('SOURCE_TABLE'),
    'TARGET_TABLE': os.getenv('TARGET_TABLE'),
    'UPSERT_QUERY': os.getenv('UPSERT_QUERY'),
    'INSERT_METHOD': os.getenv('INSERT_METHOD', 'append'),
    'PARTITION_BY': os.getenv('PARTITION_BY', 'registration_date')
}

def get_df_postgres(spark, jdbc_url, query):
    df = spark.read.format('jdbc') \
        .option("url", jdbc_url) \
        .option("query",query) \
        .option("driver","org.postgresql.Driver") \
        .load()

    return df

def get_spark_df(spark):
    df = spark.table(ENV['SOURCE_TABLE'])
    return df

def get_dataframe(spark, query):
    jdbc_url = ENV['JDBC_URL']
    extract_query = ENV['EXTRACT_QUERY']
    if 'postgres' in jdbc_url:
        df = get_df_postgres(spark, jdbc_url, query)
    elif 'mongo' in jdbc_url:
        pass
    elif extract_query != '':
        df = spark.sql(extract_query)
    else:
        df = get_spark_df(spark)
    
    return df

def transform_dataframe(df):
    # remove duplicate
    # df.drop_duplicates()
    
    # rename column created_at 
    df = df.withColumnRenamed('created_at', 'acq_channel_created_at')

    # drop redundant column (join column)
    df = df.drop('acquisition_channel_id')

    # create new timestamp column
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df.withColumn(
        'created_at',
        F.unix_timestamp(F.lit(timestamp),'yyyy-MM-dd HH:mm:ss').cast('timestamp')
    )

    # add country code to phone number
    return df

def upsert_dataframe(spark, query):
    spark.sql(query)
        
def load_dataframe(df, table, method, **kwargs):
    # Write data to MinIO in Iceberg format
    try:
        if method == 'overwrite':
            partition_by = kwargs['partition_by']
            print(f'partition_by: {partition_by}')
            if partition_by is None:
                raise Exception('overwrite method need partition_by column!')
            df.writeTo(table).partitionedBy(partition_by).createOrReplace()

        else:
            df.writeTo(table).append()
    except Exception as e:
        raise Exception(f'load_dataframe Error: {e}')

def etl(spark):
    print(f"Extracting Data")
    df = get_dataframe(spark, ENV['EXTRACT_QUERY'])
    print(f'Data Extracted: \n{df.show(5)}')

    print("Transforming Data")
    df = transform_dataframe(df)
    print(f'Data Transformed: \n{df.show(5)}')

    print(f"Writing data to Iceberg table")
    load_dataframe(df, 
                   table=ENV['TARGET_TABLE'],
                   method=ENV['INSERT_METHOD'],
                   partition_by=F.days(ENV['PARTITION_BY'])
    )
    # upsert_dataframe(spark, ENV['UPSERT_QUERY'])

    print(f"Data Extraction has completed successfully!")


if __name__=="__main__":
    # Initialize Spark session with necessary Iceberg and Hadoop configurations
    spark = SparkSession.builder \
        .appName(ENV['SPARK_APPNAME']) \
        .getOrCreate()

    etl(spark)

    # Stop spark session
    spark.stop()
