from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import os
import logging
from dotenv import load_dotenv
import datetime

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# spark = SparkSession.builder \
#     .appName("FoodDeliveryETL") \
#     .config("spark.jars.packages", 
#             "org.apache.hadoop:hadoop-aws:3.3.6,net.snowflake:spark-snowflake_2.13:3.10") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY")) \
#     .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_KEY")) \
#     .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName("FoodDeliveryETL") \
    .config("spark.jars", ",".join([
        "./jars/snowflake-jdbc-3.13.30.jar",
        "./jars/spark-snowflake_2.12-2.11.0-spark_3.3.jar",
        "./jars/hadoop-aws-3.3.6.jar"
    ])) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

def get_orders_schema():
    return StructType([
        StructField("event_id", StringType()),
        StructField("event_timestamp", TimestampType()),
        StructField("order_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("service_type", StringType()),
        StructField("order_status", StringType()),
        StructField("items", ArrayType(StructType([
            StructField("item_id", StringType()),
            StructField("name", StringType()),
            StructField("quantity", IntegerType()),
            StructField("unit_price", FloatType()),
            StructField("total_price", FloatType())
        ]))),
        StructField("delivery_location", StructType([
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType()),
            StructField("address", StringType())
        ])),
        StructField("delivery_fee", FloatType()),
        StructField("total_amount", FloatType()),
        StructField("estimated_delivery_time", TimestampType()),
        StructField("payment_method", StringType()),
        StructField("payment_status", StringType())
    ])

def read_bronze_data(s3_path):
    logger.info(f"Reading data from {s3_path}")
    return spark.read.schema(get_orders_schema()).parquet(s3_path)

def transform_to_silver(df):
    df = df.filter(F.col("order_id").isNotNull() & F.col("merchant_id").isNotNull() & (F.col("total_amount") > 0))
    df = df.withColumn("event_date", F.date_format("event_timestamp", "yyyy-MM-dd"))
    df = df.withColumn("delivery_latitude", F.col("delivery_location.latitude")) \
           .withColumn("delivery_longitude", F.col("delivery_location.longitude")) \
           .drop("delivery_location")
    df = df.withColumn("delivery_time_minutes", 
                       F.round((F.unix_timestamp("estimated_delivery_time") - 
                                F.unix_timestamp("event_timestamp")) / 60))
    df = df.withColumn("is_delayed", F.when(F.col("delivery_time_minutes") > 45, True).otherwise(False))
    return df

def explode_order_items(df):
    return df.select("order_id", F.explode("items").alias("item"))\
        .select("order_id", "item.item_id", "item.name", "item.quantity", "item.unit_price", "item.total_price")

def write_silver_data(df, s3_path):
    logger.info(f"Writing data to {s3_path}")
    df.write.partitionBy("event_date").mode("append").parquet(s3_path)

def write_order_items(df, s3_path):
    df.write.mode("append").parquet(s3_path)

def write_to_snowflake(df, table_name):
    sf_options = {
        "sfUrl": "https://fg00255.switzerland-north.azure.snowflakecomputing.com",
        "sfUser": os.getenv("SNOWFLAKE_USER"),
        "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
        "sfDatabase": "DELIVERY",
        "sfSchema": "SILVER",
        "sfWarehouse": "COMPUTE_WH"
    }
    logger.info(f"Writing data to Snowflake table: {table_name}")
    df.write.format("net.snowflake.spark.snowflake").options(**sf_options).option("dbtable", table_name).mode("append").save()

def get_dates_in_bronze():
    """
    Function to generate list of available dates in the bronze layer
    """
    start_date = datetime.date(2025, 2, 1)  # Change to your start date
    end_date = datetime.date.today()
    
    date_list = []
    while start_date <= end_date:
        date_list.append(start_date.strftime("%Y/%m/%d"))
        start_date += datetime.timedelta(days=1)

    return date_list

def process_data_for_each_day():
    base_s3_bronze_path = "s3a://numa-delivery/bronze/food-delivery-orders-raw"
    s3_silver_orders_path = "s3a://numa-delivery/silver/food-delivery-orders/"
    s3_silver_items_path = "s3a://numa-delivery/silver/food-delivery-order_items/"
    
    date_list = get_dates_in_bronze()
    
    for date in date_list:
        s3_bronze_path = f"{base_s3_bronze_path}/{date}/*.parquet"
        
        try:
            logger.info(f"Processing data for date: {date}")
            bronze_df = read_bronze_data(s3_bronze_path)
            
            if bronze_df.count() == 0:
                logger.info(f"No data found for {date}, skipping.")
                continue
            
            silver_df = transform_to_silver(bronze_df)
            order_items_df = explode_order_items(silver_df)
            
            write_silver_data(silver_df, s3_silver_orders_path)
            write_order_items(order_items_df, s3_silver_items_path)
            
            write_to_snowflake(silver_df, "ORDER")
            write_to_snowflake(order_items_df, "ORDER_ITEMS")
        
        except Exception as e:
            logger.error(f"Error processing {date}: {e}")

def main():
    process_data_for_each_day()
    spark.stop()

if __name__ == "__main__":
    main()
