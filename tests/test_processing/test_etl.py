import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
)

from processing.write_to_silver import (get_orders_schema, transform_to_silver, explode_order_items)


class TestETL(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Setup SparkSession for testing
        cls.spark = SparkSession.builder.appName("ETL Unit Test").getOrCreate()

    def test_get_orders_schema(self):
        # Test the schema structure
        schema = get_orders_schema()
        self.assertTrue(isinstance(schema, StructType))
        self.assertEqual(len(schema.fields), 16)

    def test_transform_to_silver(self):
        # Test the transformation logic

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
        df = self.spark.createDataFrame(data, schema)

        transformed_df = transform_to_silver(df)

        # Test transformations
        self.assertTrue("event_timestamp" in transformed_df.columns)
        self.assertTrue("delivery_fee" in transformed_df.columns)
        self.assertTrue("total_amount" in transformed_df.columns)

    def test_explode_order_items(self):
        # Test explode order items transformation

        data = [
            (
                "1001",
                [
                    {
                        "item_id": "item1",
                        "name": "item_name1",
                        "quantity": 1,
                        "unit_price": "10.0",
                        "total_price": "10.0",
                    }
                ],
            ),
        ]
        df = self.spark.createDataFrame(data, ["order_id", "items"])

        exploded_df = explode_order_items(df)

        self.assertEqual(exploded_df.count(), 1)
        self.assertTrue("item_id" in exploded_df.columns)
        self.assertTrue("quantity" in exploded_df.columns)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


if __name__ == "__main__":
    unittest.main()
