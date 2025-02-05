from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, sum, count, avg
# import logging

# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__) 


# def write_to_snowflake(df, sf_options: dict):
#     """
#     Write the DataFrame to a Snowflake table.
#     Requires the Snowflake Spark Connector to be configured.
#     :param df: DataFrame to write
#     :param sf_options: Dictionary containing Snowflake options such as URL, user, password, db, schema, warehouse, and table.
#     """
#     logger.info("Writing data to Snowflake")
#     df.write.format("snowflake") \
#         .options(**sf_options) \
#         .mode("append") \
#         .save() 

# def perfomr_aggregations():
        
#         gold_df = silver_df.groupBy("merchant_id", "service_type") \
#         .agg(
#             count("order_id").alias("total_orders"),
#             sum("total_amount").alias("total_revenue"),
#             avg("total_amount").alias("average_order_value")
#         )

    
# def silver_to_gold():
#     # Initialize Spark session
#     spark = SparkSession.builder \
#         .appName("SilverToGold") \
#         .getOrCreate()

#     # Read cleaned data from silver layer
#     silver_df = spark.read \
#         .format("parquet") \
#         .load("s3a://your-bucket/silver/orders/")

#     # Perform aggregations for gold layer

#     # Write aggregated data to gold layer (e.g., S3 or Snowflake)
#     gold_df.write \
#         .format("parquet") \
#         .mode("overwrite") \
#         .save("s3a://your-bucket/gold/merchant_metrics/")
    
#         # Define Snowflake options. Adjust these parameters as needed.
#     sf_options = {
#         "sfURL": "your_account.snowflakecomputing.com",
#         "sfUser": "YOUR_USERNAME",
#         "sfPassword": "YOUR_PASSWORD",
#         "sfDatabase": "YOUR_DATABASE",
#         "sfSchema": "YOUR_SCHEMA",
#         "sfWarehouse": "YOUR_WAREHOUSE",
#         "dbtable": "ORDERS"
#     }

#     # Write to Snowflake
#     write_to_snowflake(gold_df, sf_options)

#     spark.stop()
    

    

#     spark.stop()

# if __name__ == "__main__":
#     silver_to_gold()