import os
import sys
import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

sys.path.insert(1, f'{os.getenv('PROJECT_PATH')}/spark')
from jobs.curated_customers import get_df_postgres, get_spark_df, get_dataframe, transform_dataframe, load_dataframe, etl

JDBC_URL = "jdbc:postgresql://postgres/ecommerce"

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .master(os.getenv('SPARK_MASTER', 'local[1]')) \
        .appName("test curated customers job") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_get_df_postgres(spark):
    query = "SELECT * FROM customers LIMIT 10"
    with patch('pyspark.sql.DataFrameReader.load', return_value=MagicMock()) as mock_load:
        df = get_df_postgres(spark, JDBC_URL, query)
        mock_load.assert_called_once()
        assert df.count() == 10

def test_get_spark_df(spark):
    with patch.dict('ENV', {'SOURCE_TABLE': 'demo.test_raw_ecommerce.customers'}):
        with patch('pyspark.sql.SparkSession.table', return_value=MagicMock()) as mock_table:
            df = get_spark_df(spark)
            mock_table.assert_called_once_with('demo.test_raw_ecommerce.customers')
            assert df is not None


def test_transform_dataframe(spark):
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("country", StringType(), True),
        StructField("registration_date", StringType(), True),
        StructField("acquisition_channel_id", StringType(), True)
    ])
    data = [("11", "Johnny Does", "M", "john@example.com", "1234567890", "US", "2021-01-01", "1")]
    df = spark.createDataFrame(data, schema)
    transformed_df = transform_dataframe(df)
    # check if the name is split correctly
    assert "first_name" in transformed_df.columns
    assert transformed_df.collect()[0]["first_name"] == "Johnny"
    assert "last_name" in transformed_df.columns
    assert transformed_df.collect()[0]["last_name"] == "Does"
    # check if the phone number is transformed correctly
    assert transformed_df.collect()[0]["phone"] == "+11234567890"

def test_load_dataframe(spark):
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("country", StringType(), True),
        StructField("registration_date", StringType(), True),
        StructField("acquisition_channel_id", StringType(), True)
    ])
    data = [("123", "Johnny Does", "M", "john@example.com", "+11234567890", "US", "2021-01-01", "1")]
    df = spark.createDataFrame(data, schema)
    with patch('pyspark.sql.DataFrameWriterV2.createOrReplace', return_value=None) as mock_createOrReplace:
        load_dataframe(df, "demo.test_curated_ecommerce.customers", "overwrite", partition_by="registration_date")
        mock_createOrReplace.assert_called_once()

def test_etl(spark):
    with patch.dict('ENV', {
        'EXTRACT_QUERY': 'SELECT * FROM demo.test_raw_ecommerce.customers',
        'TARGET_TABLE': 'demo.test_curated_ecommerce.customers',
        'INSERT_METHOD': 'overwrite',
        'PARTITION_BY': 'registration_date'
    }):
        with patch('pyspark.sql.DataFrameReader.load', return_value=MagicMock()) as mock_load:
            with patch('pyspark.sql.DataFrameWriterV2.createOrReplace', return_value=None) as mock_createOrReplace:
                etl(spark)
                mock_load.assert_called_once()
                mock_createOrReplace.assert_called_once()