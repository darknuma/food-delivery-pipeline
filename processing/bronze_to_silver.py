from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, from_json, explode, sum, count, min, max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DoubleType, ArrayType
import logging
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_silver_data(spark: SparkSession, s3_path: str):
    """
    Load cleaned Bronze data from S3.
    :param spark: SparkSession
    :param s3_path: S3 path where the cleaned Bronze data is stored.
    :return: DataFrame
    """
    logger.info(f"Loading cleaned Bronze data from {s3_path}")
    return spark.read.parquet(s3_path)


def transform_orders_data(df):
    """
    Transform Bronze Layer data into Silver Layer format.
    :param df: Input DataFrame
    :return: Transformed DataFrame
    """
    logger.info("Transforming Bronze data to Silver Layer")

    # Define schema for items array (nested JSON structure)
    items_schema = ArrayType(StructType([
        StructField("item_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DecimalType(10, 2), False),
        StructField("total_price", DecimalType(10, 2), False)
    ]))

    # Define schema for delivery location
    location_schema = StructType([
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("address", StringType(), False)
    ])

    # Transform data
    df_transformed = df \
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp"))) \
        .withColumn("estimated_delivery_time", to_timestamp(col("estimated_delivery_time"))) \
        .withColumn("items", from_json(col("items"), items_schema)) \
        .withColumn("delivery_location", from_json(col("delivery_location"), location_schema))

    # Flatten items array
    df_flattened = df_transformed \
        .withColumn("item", explode(col("items"))) \
        .select(
            col("order_id"),
            col("customer_id"),
            col("event_timestamp"),
            col("estimated_delivery_time"),
            col("delivery_location.latitude"),
            col("delivery_location.longitude"),
            col("delivery_location.address"),
            col("item.item_id"),
            col("item.name").alias("item_name"),
            col("item.quantity"),
            col("item.unit_price"),
            col("item.total_price")
        )

    return df_flattened


def aggregate_orders(df):
    """
    Aggregate order-level data.
    :param df: DataFrame with flattened items.
    :return: Aggregated DataFrame
    """
    logger.info("Aggregating Silver Layer data")

    df_aggregated = df.groupBy("order_id").agg(
        count("*").alias("total_items"),
        sum("total_price").alias("total_order_value"),
        min("event_timestamp").alias("order_time"),
        max("estimated_delivery_time").alias("max_estimated_delivery")
    )

    return df_aggregated


def write_to_snowflake(df, sf_options: dict, table_name: str):
    """
    Write the DataFrame to a Snowflake table.
    :param df: DataFrame to write
    :param sf_options: Snowflake connection options
    :param table_name: Target Snowflake table
    """
    logger.info(f"Writing data to Snowflake table {table_name}")
    sf_options["dbtable"] = table_name

    df.write.format("snowflake") \
        .options(**sf_options) \
        .mode("append") \
        .save()


def main():
    spark = SparkSession.builder \
        .appName("S3 Access Example") \
        .config("spark.hadoop.fs.s3a.access.key", "YOUR_AWS_ACCESS_KEY_ID") \
        .config("spark.hadoop.fs.s3a.secret.key", "YOUR_AWS_SECRET_ACCESS_KEY") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    bucket = "numa-delivery"
    topic = "food-delivery-orders-raw" 
    s3_bronze_path = f"s3a://{bucket}/bronze/{topic}/*/*.parquet"

    df_bronze = load_silver_data(spark, s3_bronze_path)

    df_silver = transform_orders_data(df_bronze)
    df_aggregated = aggregate_orders(df_silver)

    sf_options = {
        "sfUrl": "https://fg00255.switzerland-north.azure.snowflakecomputing.com",
        "sfUser": os.getenv("SNOWFLAKE_USER"),
        "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
        "sfDatabase": "delivery",
        "sfSchema": "SILVER",
        "sfWarehouse": "COMPUTE_WH"
    }

    write_to_snowflake(df_silver, sf_options, "SILVER_ORDERS")
    write_to_snowflake(df_aggregated, sf_options, "AGGREGATED_ORDERS")

    spark.stop()


if __name__ == "__main__":
    main()
