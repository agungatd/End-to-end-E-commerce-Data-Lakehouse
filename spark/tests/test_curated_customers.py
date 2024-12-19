import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from .curated_customers import get_df_postgres, get_spark_df, get_dataframe, transform_dataframe, load_dataframe, etl

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    yield spark
    spark.stop()

def test_get_df_postgres(spark):
    jdbc_url = "jdbc:postgresql://localhost/test"
    query = "SELECT * FROM test_table"
    with patch('pyspark.sql.DataFrameReader.load', return_value=MagicMock()) as mock_load:
        df = get_df_postgres(spark, jdbc_url, query)
        mock_load.assert_called_once()
        assert df is not None

def test_get_spark_df(spark):
    with patch.dict('os.environ', {'SOURCE_TABLE': 'test_table'}):
        with patch('pyspark.sql.SparkSession.table', return_value=MagicMock()) as mock_table:
            df = get_spark_df(spark)
            mock_table.assert_called_once_with('test_table')
            assert df is not None

def test_get_dataframe(spark):
    with patch.dict('os.environ', {'JDBC_URL': 'jdbc:postgresql://localhost/test', 'SOURCE_TABLE': 'test_table'}):
        with patch('pyspark.sql.DataFrameReader.load', return_value=MagicMock()) as mock_load:
            df = get_dataframe(spark, "SELECT * FROM test_table")
            mock_load.assert_called_once()
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
    data = [("1", "John Doe", "M", "john@example.com", "1234567890", "US", "2021-01-01", "1")]
    df = spark.createDataFrame(data, schema)
    transformed_df = transform_dataframe(df)
    assert "first_name" in transformed_df.columns
    assert "last_name" in transformed_df.columns

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
    data = [("1", "John Doe", "M", "john@example.com", "1234567890", "US", "2021-01-01", "1")]
    df = spark.createDataFrame(data, schema)
    with patch('pyspark.sql.DataFrameWriterV2.createOrReplace', return_value=None) as mock_createOrReplace:
        load_dataframe(df, "test_table", "overwrite", partition_by="registration_date")
        mock_createOrReplace.assert_called_once()

def test_etl(spark):
    with patch.dict('os.environ', {
        'EXTRACT_QUERY': 'SELECT * FROM test_table',
        'TARGET_TABLE': 'test_table',
        'INSERT_METHOD': 'overwrite',
        'PARTITION_BY': 'registration_date'
    }):
        with patch('pyspark.sql.DataFrameReader.load', return_value=MagicMock()) as mock_load:
            with patch('pyspark.sql.DataFrameWriterV2.createOrReplace', return_value=None) as mock_createOrReplace:
                etl(spark)
                mock_load.assert_called_once()
                mock_createOrReplace.assert_called_once()