from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg

def silver_to_gold():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("SilverToGold") \
        .getOrCreate()

    # Read cleaned data from silver layer
    silver_df = spark.read \
        .format("parquet") \
        .load("s3a://your-bucket/silver/orders/")

    # Perform aggregations for gold layer
    gold_df = silver_df.groupBy("merchant_id", "service_type") \
        .agg(
            count("order_id").alias("total_orders"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("average_order_value")
        )

    # Write aggregated data to gold layer (e.g., S3 or Snowflake)
    gold_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .save("s3a://your-bucket/gold/merchant_metrics/")

    spark.stop()

if __name__ == "__main__":
    silver_to_gold()