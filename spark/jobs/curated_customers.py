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
    'PARTITION_BY': os.getenv('PARTITION_BY', 'created_at')
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
    if 'postgres' in jdbc_url:
        df = get_df_postgres(spark, jdbc_url, query)
    else:
        df = get_spark_df(spark)
    
    return df

def transform_dataframe(df):
    # remove duplicate
    df.drop_duplicates()

    # add dial code to msisdn
    def map_phone_code(phone, country_code):
        if phone.startswith('+'):
            return phone

        with open('/home/spark/jobs/helpers/country_phone_code.json', 'r') as f:
            data = json.load(f)
        for c in data:
            if c['code'] == country_code:
                return c['dial_code'] + phone
        return None
    fix_phone_code_udf = F.udf(map_phone_code, StringType())
    df = df.withColumn('phone', fix_phone_code_udf(F.col('phone'), F.col('country')))

    # seperate first and last name from full name
    def get_first_last_name(name):
        names = name.split()
        if len(names) == 1:
            return Row('Out1', 'Out2') (names[0], names[0])
        elif len(names) > 2:
            return Row('Out1', 'Out2') (names[0], names[-1])
        else:
            return Row('Out1', 'Out2') (names[0], names[1])
    
    # Assign the structure and naming for newly created columns
    schema = StructType([StructField("first_name",
                                    StringType(), False),
                        StructField("last_name",
                                    StringType(), False)])

    split_name_udf = F.udf(get_first_last_name, schema)
    df = df.withColumn("Result",
                split_name_udf(df["name"]))
    df = df.select("customer_id", "name",
                          "gender", "email",
                          "phone", "country",
                          "registration_date", "acquisition_channel_id",
                          "Result.*")

    # add country code to phone number
    return df

def upsert_dataframe(spark, query):
    spark.sql(query)
        
def load_dataframe(df, table, method, **kwargs):
    # Write data to MinIO in Iceberg format
    try:
        if method == 'overwrite':
            partition_by = kwargs['partition_by']
            if not partition_by:
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
