from pyspark.sql import SparkSession

def migrate(spark, jdbc_url,  table_name):
    # Read data from PostgreSQL
    print("Reading data from PostgreSQL...")
    postgres_df = spark.read.format('jdbc') \
                    .option("url", jdbc_url) \
                    .option("dbtable",f"ecommerce.public.{table_name}") \
                    .option("user","postgres") \
                    .option("password","postgres") \
                    .option("driver","org.postgresql.Driver") \
                    .load()

    # Show a preview of the data
    postgres_df.show(5)

    # Write data to MinIO in Iceberg format
    iceberg_table = f"demo.dev_raw_ecommerce.{table_name}"

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
    tables = ["customer_acquisition_channels",
              "customers",
              "inventory",
              "order_items",
              "orders",
              "product_categories",
              "products"]

    # PostgreSQL connection properties
    jdbc_url = "jdbc:postgresql://postgres/ecommerce"
    for table in tables:
        migrate(spark, jdbc_url, table)

    # Stop spark session
    spark.stop()