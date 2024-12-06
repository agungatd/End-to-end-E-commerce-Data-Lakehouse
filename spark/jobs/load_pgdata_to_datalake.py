import os
from pyspark.sql import SparkSession

def get_pg_df(spark, jdbc_url, query):
    df = spark.read.format('jdbc') \
            .option("url", jdbc_url) \
            .option("query",query) \
            .option("user","postgres") \
            .option("password","postgres") \
            .option("driver","org.postgresql.Driver") \
            .load()

    return df

def get_data_latest_date(spark, jdbc_url, table, col):
    query = f"SELECT MAX({col}) FROM {table}"
    df = get_pg_df(spark, jdbc_url, query)

    return df.first()[0]

def migrate(spark, jdbc_url, schema, table):
    if table == 'customers':
        filter_col = 'registration_date'
    else:
        filter_col = 'created_at'
    # Set the cutoff time from latest data created datetime.
    cutoff_time = get_data_latest_date(spark, jdbc_url, table, filter_col)

    print(f"Reading data {table} with cutoff {cutoff_time} from PostgreSQL...")
    query = f"""
        SELECT * FROM {table}
        WHERE {filter_col} > '{cutoff_time}'
    """
    # Read data from PostgreSQL
    df = get_pg_df(spark, jdbc_url, query)

    # Show a preview of the data
    # df.show(5)

    # Write data to MinIO in Iceberg format
    iceberg_table = f"demo.{schema}.{table}"

    print(f"Writing data to Iceberg table: {iceberg_table}")
    df.writeTo(iceberg_table) \
        .append()

    print(f"Data migration for table {table} completed successfully!")


if __name__=="__main__":
    table = os.getenv('POSTGRES_TABLE')

    # Initialize Spark session with necessary Iceberg and Hadoop configurations
    spark = SparkSession.builder \
        .appName(f"PostgreSQL to MinIO with Iceberg DataLakehouse, table {table}") \
        .getOrCreate()
    schema = "ecommerce"

    # PostgreSQL connection properties
    jdbc_url = "jdbc:postgresql://postgres/ecommerce"
    
    migrate(spark, jdbc_url, schema, table)

    # Stop spark session
    spark.stop()