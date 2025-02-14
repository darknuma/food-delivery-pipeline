import pytest
from pyspark.sql import SparkSession
from your_module import write_to_snowflake, execute_snowflake_query

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("SnowflakeTransformTests") \
        .getOrCreate()

@pytest.fixture(scope="module")
def mock_orders_df(spark):
    # Mock DataFrame representing transformed data from the 'orders' table
    data = [("2023-02-10", "order1", 100, "DELIVERED", "customer1", "merchant1")]
    return spark.createDataFrame(data, ["event_date", "order_id", "total_amount", "order_status", "customer_id", "merchant_id"])

@pytest.fixture(scope="module")
def mock_order_items_df(spark):
    # Mock DataFrame representing transformed data from the 'order_items' table
    data = [("order1", "item1", 1, 25, "food", "DELIVERED")]
    return spark.createDataFrame(data, ["order_id", "item_id", "quantity", "unit_price", "category", "order_status"])

def test_write_to_snowflake(mock_orders_df, mock_order_items_df):
    # Test that the write function correctly writes to Snowflake
    try:
        # Assuming `write_to_snowflake` merges and writes to Snowflake tables
        write_to_snowflake(mock_orders_df, "ORDERS")
        write_to_snowflake(mock_order_items_df, "ORDER_ITEMS")
        
        # Check if the tables are populated after the write operation
        result = execute_snowflake_query("SELECT COUNT(*) FROM ORDERS", return_results=True)
        assert result[0][0] > 0, "Orders table is empty after write"

        result = execute_snowflake_query("SELECT COUNT(*) FROM ORDER_ITEMS", return_results=True)
        assert result[0][0] > 0, "Order items table is empty after write"
    
    except Exception as e:
        pytest.fail(f"Write operation failed: {e}")

def test_merge_snowflake_data():
    # Run a simple merge query test
    merge_query = """
    MERGE INTO ORDERS AS target
    USING ORDERS_STAGING AS source
    ON target.order_id = source.order_id
    WHEN MATCHED THEN UPDATE SET target.total_amount = source.total_amount
    WHEN NOT MATCHED THEN INSERT (order_id, total_amount) VALUES (source.order_id, source.total_amount)
    """
    
    try:
        # Execute the merge and validate the effect
        execute_snowflake_query([merge_query])
        
        # Verify the data in Snowflake after the merge operation
        result = execute_snowflake_query("SELECT total_amount FROM ORDERS WHERE order_id = 'order1'", return_results=True)
        assert result, "Merge operation didn't update data as expected"
    
    except Exception as e:
        pytest.fail(f"Merge operation failed: {e}")

def test_fulfillment_rate_calculation(mock_orders_df):
    from your_module import calculate_fulfillment_metrics

    # Apply the metric calculation
    result_df = calculate_fulfillment_metrics(mock_orders_df)

    # Collect the result for validation
    result = result_df.collect()

    # Validate the fulfillment rate
    fulfillment_rate = result[0]["fulfillment_rate"]
    assert fulfillment_rate == 100.0, f"Expected 100.0% fulfillment rate, but got {fulfillment_rate}%"

