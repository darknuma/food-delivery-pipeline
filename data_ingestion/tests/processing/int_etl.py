import unittest
from unittest.mock import patch
from moto import mock_s3
import boto3
from pyspark.sql import SparkSession
from your_module import (
    read_bronze_data,
    write_silver_data,
    transform_to_silver,
    get_orders_schema
)

class TestS3ETL(unittest.TestCase):

    @mock_s3
    def setUp(self):
        # Mock S3
        self.s3_client = boto3.client("s3", region_name="us-east-2")
        self.s3_client.create_bucket(Bucket="numa-delivery")
        
        # Setup SparkSession for testing
        self.spark = SparkSession.builder \
            .appName("ETL Integration Test") \
            .getOrCreate()
        
        # Sample data
        data = [
            ("1", "2025-02-10 10:00:00.000000", "1001", "merchant1", "customer1", "service1", "delivered", [], 
             {"latitude": 10.0, "longitude": 20.0, "address": "address1"}, "5.0", "100.0", "2025-02-10 10:45:00.000000", "credit_card", "paid")
        ]
        schema = get_orders_schema()
        self.df = self.spark.createDataFrame(data, schema)
    
    @mock_s3
    def test_read_and_write_s3(self):
        # Write test data to mock S3
        write_path = "s3a://numa-delivery/bronze/food-delivery-orders-raw/2025/02/10"
        self.df.write.mode("overwrite").parquet(write_path)
        
        # Read the data back
        read_df = read_bronze_data(write_path)
        
        # Assert the data has been written and read back correctly
        self.assertEqual(read_df.count(), 1)
        self.assertEqual(read_df.columns[0], "event_id")

    @mock_s3
    def test_etl_process(self):
        # Test the complete ETL process with transformation and S3 interactions
        base_s3_bronze_path = "s3a://numa-delivery/bronze/food-delivery-orders-raw"
        s3_silver_orders_path = "s3a://numa-delivery/silver/food-delivery-orders/"
        
        write_path = f"{base_s3_bronze_path}/2025/02/10"
        self.df.write.mode("overwrite").parquet(write_path)

        # Transform to Silver data
        bronze_df = read_bronze_data(write_path)
        silver_df = transform_to_silver(bronze_df)
        
        # Write Silver data
        write_silver_data(silver_df, s3_silver_orders_path)

        # Verify the data is written to the Silver layer
        silver_data_path = f"{s3_silver_orders_path}/2025/02/10"
        silver_df_read = self.spark.read.parquet(silver_data_path)
        
        self.assertEqual(silver_df_read.count(), 1)
        self.assertTrue("event_date" in silver_df_read.columns)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

if __name__ == "__main__":
    unittest.main()
