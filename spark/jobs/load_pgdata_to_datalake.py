from pyspark.sql import SparkSession

def migrate(spark, jdbc_url, schema,  table_name):
    # Read data from PostgreSQL
    print("Reading data from PostgreSQL...")
    if table_name == 'customers':
        filter_col = 'registration_date'
    else:
        filter_col = 'created_at'

    query = f"""
        select * from {table}
        where {filter_col} > '2020-01-05 02:23:48.000'
    """
    postgres_df = spark.read.format('jdbc') \
                    .option("url", jdbc_url) \
                    .option("query",query) \
                    .option("user","postgres") \
                    .option("password","postgres") \
                    .option("driver","org.postgresql.Driver") \
                    .load()

    # Show a preview of the data
    postgres_df.show(5)

    # Write data to MinIO in Iceberg format
    iceberg_table = f"demo.{schema}.{table_name}"

    print(f"Writing data to Iceberg table: {iceberg_table}")
    postgres_df \
        .writeTo(iceberg_table) \
        .append() \

    print("Data migration completed successfully!")


if __name__=="__main__":
    # Initialize Spark session with necessary Iceberg and Hadoop configurations
    spark = SparkSession.builder \
        .appName("PostgreSQL to MinIO with Iceberg DataLakehouse") \
        .getOrCreate()
    schema = "ecommerce"
    tables = ["customer_acquisition_channels", "customers", "inventory",
              "order_items", "orders", "product_categories", "products"]
    tables = ['customers']

    # PostgreSQL connection properties
    jdbc_url = "jdbc:postgresql://postgres/ecommerce"
    # jdbc_properties = {
    #     "user": "postgres",
    #     "password": "postgres",
    #     "driver": "org.postgresql.Driver"
    # }
    for table in tables:
        migrate(spark, jdbc_url, schema, table)

    # Stop spark session
    spark.stop()