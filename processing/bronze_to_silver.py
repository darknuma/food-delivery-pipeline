from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 

# Initialize Spark session with AWS credentials  
spark = SparkSession.builder \
    .appName("Read Parquet from S3") \
    .config("spark.hadoop.fs.s3a.access.key", "<YOUR_ACCESS_KEY>") \
    .config("spark.hadoop.fs.s3a.secret.key", "<YOUR_SECRET_KEY>") \
    .getOrCreate()  

# Read Parquet file from S3  
df = spark.read.parquet("s3a://numa-delivery/bronze/orders/parquet-file")  

# Show the DataFrame  
df.show()  


from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType





# Define schema for raw data
order_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_timestamp", TimestampType(), False),
    StructField("order_id", StringType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("service_type", StringType(), False),
    StructField("order_status", StringType(), False),
    StructField("items", StringType(), False),  # JSON string
    StructField("delivery_location", StringType(), False),  # JSON string
    StructField("delivery_fee", DecimalType(10, 2), False),
    StructField("total_amount", DecimalType(10, 2), False),
    StructField("estimated_delivery_time", TimestampType(), False),
    StructField("payment_method", StringType(), False),
    StructField("payment_status", StringType(), False)
])

def bronze_to_silver():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("BronzeToSilver") \
        .getOrCreate()

    # Read raw data from bronze layer (e.g., S3 or Cassandra)
    raw_df = spark.read \
        .format("parquet") \
        .schema(order_schema) \
        .load("s3a://numa-delivery/bronze/orders/")

    # Clean and transform data
    silver_df = raw_df \
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp"))) \
        .withColumn("estimated_delivery_time", to_timestamp(col("estimated_delivery_time"))) \
        .withColumn("items", from_json(col("items"), "array<struct<item_id:string,name:string,quantity:int,unit_price:decimal(10,2),total_price:decimal(10,2)>>")) \
        .withColumn("delivery_location", from_json(col("delivery_location"), "struct<latitude:double,longitude:double,address:string>")) \
        .filter(col("order_status").isin(["created", "accepted", "delivered"]))  # Filter valid statuses

    # Write cleaned data to silver layer (e.g., S3 or Cassandra)
    silver_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .save("s3a://your-bucket/silver/orders/")

    spark.stop()

if __name__ == "__main__":
    bronze_to_silver()