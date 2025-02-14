import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from processing.write_to_silver import (
    get_orders_schema,
    transform_to_silver,
    read_bronze_data,
)


@pytest.fixture(scope="module")
def spark():
    # Setup Spark session for testing
    spark = SparkSession.builder.appName("ETL Validation Tests").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_data(spark):
    # Sample data for testing
    data = [
        (
            "1",
            "2025-02-10 10:00:00.000000",
            "1001",
            "merchant1",
            "customer1",
            "service1",
            "delivered",
            [],
            {"latitude": 10.0, "longitude": 20.0, "address": "address1"},
            "5.0",
            "100.0",
            "2025-02-10 10:45:00.000000",
            "credit_card",
            "paid",
        )
    ]
    schema = get_orders_schema()
    return spark.createDataFrame(data, schema)


def test_data_quality(sample_data):
    # Check for missing or null values in essential columns
    assert sample_data.filter(col("order_id").isNull()).count() == 0
    assert sample_data.filter(col("merchant_id").isNull()).count() == 0
    assert (
        sample_data.filter(col("total_amount") <= 0).count() == 0
    )  # Check for invalid amounts


def test_schema_validation(sample_data):
    # Validate schema by checking column names and types
    expected_columns = [
        "event_id",
        "event_timestamp",
        "order_id",
        "merchant_id",
        "customer_id",
        "service_type",
        "order_status",
        "items",
        "delivery_location",
        "delivery_fee",
        "total_amount",
        "estimated_delivery_time",
        "payment_method",
        "payment_status",
    ]

    # Check column names
    assert set(sample_data.columns) == set(expected_columns)

    # Check data types
    assert dict(sample_data.dtypes)["total_amount"] == "string"
    assert dict(sample_data.dtypes)["event_timestamp"] == "string"


def test_business_rules(sample_data):
    # Apply transformation to test business logic
    transformed_df = transform_to_silver(sample_data)

    # Business Rule: If delivery_time_minutes > 45, is_delayed should be True
    transformed_df = transformed_df.withColumn(
        "is_delayed", when(col("delivery_time_minutes") > 45, True).otherwise(False)
    )

    # Validate the business rule
    assert transformed_df.filter(col("is_delayed") == True).count() == 1


def test_end_to_end_flow(spark, sample_data):
    # End-to-end test: Simulate reading from Bronze, transforming, and writing to Silver

    # Write to a mock location
    sample_data.write.mode("overwrite").parquet("s3a://mock-bucket/bronze/")

    # Read the data from mock location
    bronze_df = read_bronze_data("s3a://mock-bucket/bronze/")

    # Apply transformation
    silver_df = transform_to_silver(bronze_df)

    # Verify the transformation and schema after loading into silver
    assert silver_df.count() > 0
    assert "event_date" in silver_df.columns

    # Further business rule validation after transformation
    transformed_df = silver_df.withColumn(
        "is_delayed", when(col("delivery_time_minutes") > 45, True).otherwise(False)
    )

    assert transformed_df.filter(col("is_delayed") == True).count() == 1
