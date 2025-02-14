import boto3
import pytest
from processing.write_to_silver import process_data_for_each_day 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark = SparkSession.builder.appName("ETL Integration Test").getOrCreate() p

@pytest.fixture
def s3_client():
    # Use boto3 to interact with S3 (ensure AWS credentials are set in environment)
    return boto3.client("s3", region_name="us-east-2")


def test_process_data_for_each_day(s3_client):
    # Test a small sample data scenario in the real S3 environment

    # You would upload sample parquet files to the S3 bucket
    # S3 paths would be the actual ones used during real processing
    base_s3_bronze_path = "s3://your-bucket/bronze/food-delivery-orders-raw"
    s3_silver_orders_path = "s3://your-bucket/silver/food-delivery-orders"
    s3_silver_items_path = "s3://your-bucket/silver/food-delivery-order_items"

    # Process data
    process_data_for_each_day()

    # Validate that the output exists in S3
    result_df = spark.read.parquet(s3_silver_orders_path)
    assert result_df.count() > 0

    # Check if the silver data has been written to the right path
    response = s3_client.list_objects_v2(
        Bucket="your-bucket", Prefix="silver/food-delivery-orders"
    )
    assert "Contents" in response
    assert len(response["Contents"]) > 0
