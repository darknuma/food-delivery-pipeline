import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, DoubleType
import pyspark.sql.functions as F
from unittest.mock import patch
from datetime import datetime 
from processing.write_to_silver import (
    transform_to_silver, explode_order_items,
    write_order_items, read_bronze_data,
    write_silver_data, get_orders_schema
)


# Mock data for testing
@pytest.fixture
def mock_data(spark):
    schema = StructType([
        StructField("event_id", StringType()),
        StructField("event_timestamp", StringType()),
        StructField("order_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("service_type", StringType()),
        StructField("order_status", StringType()),
        StructField("items", ArrayType(StructType([
            StructField("item_id", StringType()),
            StructField("name", StringType()),
            StructField("quantity", DoubleType()),
            StructField("unit_price", StringType()),
            StructField("total_price", StringType())
        ]))),
        StructField("delivery_location", StructType([
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType()),
            StructField("address", StringType())
        ])),
        StructField("delivery_fee", StringType()),
        StructField("total_amount", StringType()),
        StructField("estimated_delivery_time", StringType()),
        StructField("payment_method", StringType()),
        StructField("payment_status", StringType())
    ])

    # Sample data for the test
    data = [
        Row(event_id="1", event_timestamp="2025-02-10 10:00:00.000000", order_id="101", merchant_id="1", customer_id="C1",
            service_type="delivery", order_status="completed", items=[{"item_id": "item1", "name": "burger", "quantity": 1, "unit_price": "5.99", "total_price": "5.99"}],
            delivery_location={"latitude": 40.7128, "longitude": -74.0060, "address": "123 Main St"},
            delivery_fee="2.50", total_amount="8.49", estimated_delivery_time="2025-02-10 10:30:00.000000", payment_method="credit", payment_status="paid")
    ]
    
    return spark.createDataFrame(data, schema)

def test_transform_to_silver(mock_data):
    result_df = transform_to_silver(mock_data)
    
    assert result_df.count() == 1
    assert "event_timestamp" in result_df.columns
    assert "estimated_delivery_time" in result_df.columns
    assert "delivery_fee" in result_df.columns
    assert "total_amount" in result_df.columns
    assert "event_date" in result_df.columns
    assert "delivery_latitude" in result_df.columns
    assert "delivery_longitude" in result_df.columns
    assert "delivery_time_minutes" in result_df.columns
    assert "is_delayed" in result_df.columns
    
    # Check the calculated fields
    first_row = result_df.collect()[0]
    assert first_row["is_delayed"] is False
    assert isinstance(first_row["event_timestamp"], datetime)

def test_explode_order_items(mock_data):
    silver_df = transform_to_silver(mock_data)
    exploded_df = explode_order_items(silver_df)

    assert exploded_df.count() == 1
    assert "order_id" in exploded_df.columns
    assert "item_id" in exploded_df.columns
    assert "quantity" in exploded_df.columns
    assert "unit_price" in exploded_df.columns
    assert "total_price" in exploded_df.columns
    
    first_row = exploded_df.collect()[0]
    assert first_row["order_id"] == "101"
    assert first_row["item_id"] == "item1"
    assert first_row["quantity"] == 1
    assert first_row["unit_price"] == 5.99
    assert first_row["total_price"] == 5.99

def test_read_bronze_data(spark):
    mock_path = "s3a://numa-delivery/bronze/food-delivery-orders-raw/2025/02/10/*"
    
    # Mock the S3 reading operation
    with patch("pyspark.sql.DataFrameReader.parquet") as mock_parquet:
        mock_parquet.return_value = spark.createDataFrame([], get_orders_schema())  # return empty DataFrame
        
        df = read_bronze_data(mock_path)
        
        mock_parquet.assert_called_once_with(mock_path)
        assert df.count() == 0

def test_write_silver_data(spark):
    silver_df = spark.createDataFrame([Row(order_id="101")], StructType([StructField("order_id", StringType())]))
    mock_path = "s3a://numa-delivery/silver/food-delivery-orders/2025/02/10/"
    
    with patch("pyspark.sql.DataFrameWriter.parquet") as mock_parquet:
        write_silver_data(silver_df, mock_path)
        
        mock_parquet.assert_called_once_with(mock_path)
