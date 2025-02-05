# from pyspark.sql import SparkSession
# import os
# import logging
# import snowflake.connector
# from dotenv import load_dotenv
# from datetime import datetime, 
# os.environ["SNOWFLAKE_OCSP_FAIL_OPEN"] = "true"

# # Load environment variables
# load_dotenv()

# # Configure logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # Initialize Spark Session
# spark = SparkSession.builder \
#     .appName("WriteToSnowflake") \
#     .config("spark.jars.packages", 
#             "net.snowflake:snowflake-jdbc:3.13.14,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3") \
#     .getOrCreate()

# # Snowflake connection options
# SF_OPTIONS = {
#     "sfURL": "https://fg00255.switzerland-north.azure.snowflakecomputing.com",
#     "sfDatabase": "DELIVERY",
#     "sfSchema": "SILVER",
#     "sfWarehouse": "COMPUTE_WH",
#     "sfUser": os.getenv("SNOWFLAKE_USER"),
#     "sfPassword": os.getenv("SNOWFLAKE_PASSWORD")
# }

# def execute_snowflake_query(query):
#     """Execute SQL query using Snowflake Connector."""
#     try:
#         conn = snowflake.connector.connect(
#             user=SF_OPTIONS["sfUser"],
#             password=SF_OPTIONS["sfPassword"],
#             account="fg00255.switzerland-north.azure"
#         )
#         cursor = conn.cursor()
#         cursor.execute(query)
#         conn.commit()
#         cursor.close()
#         conn.close()
#         logger.info("Query executed successfully:\n" + query)
#     except Exception as e:
#         logger.error(f"Error executing Snowflake query: {e}")
#         raise

# def write_to_snowflake(df, table_name, merge_condition):
#     """Write DataFrame to Snowflake staging table and merge into the main table."""
#     if df is None or df.isEmpty():
#         logger.info(f"No data to write for {table_name}. Skipping...")
#         return

#     temp_table = f"{table_name}_STAGING"

#     # Step 1: Write DataFrame to Snowflake staging table
#     df.write \
#         .format("snowflake") \
#         .options(**SF_OPTIONS) \
#         .option("dbtable", temp_table) \
#         .mode("overwrite") \
#         .save()
    
#     primary_keys = ['order_id']
#     if "item_id" in df.columns:
#         primary_keys.append("item_id")

#     update_columns = [f"target.{col} = source.{col}" for col in df.columns if col not in primary_keys]
#     insert_columns = ', '.join(df.columns)
#     insert_values = ', '.join([f"source.{col}" for col in df.columns])

#     # # Step 2: Generate and Execute MERGE Query via Snowflake Connector
#     # update_columns = [f"target.{col} = source.{col}" for col in df.columns if col != 'order_id']
#     # insert_columns = ', '.join(df.columns)
#     # insert_values = ', '.join([f"source.{col}" for col in df.columns])

#     merge_query = f"""
#     MERGE INTO {table_name} AS target 
#     USING {temp_table} AS source
#     ON {merge_condition}
#     WHEN MATCHED THEN
#         UPDATE SET {', '.join(update_columns)}
#     WHEN NOT MATCHED THEN
#         INSERT ({insert_columns})
#         VALUES ({insert_values})
#     """

#     logger.info(f"Executing MERGE query:\n{merge_query}")
#     execute_snowflake_query(merge_query)

#     # Step 3: Drop Staging Table After Merge
#     execute_snowflake_query(f"DROP TABLE IF EXISTS {temp_table}")

# def main():
#     try:
#         # Load Silver Data
#         s3_silver_orders_path = "s3a://numa-delivery/silver/food-delivery-orders/"
#         orders_df = spark.read.parquet(s3_silver_orders_path)

#         s3_silver_items_path = "s3a://numa-delivery/silver/food-delivery-order_items/"
#         order_items_df = spark.read.parquet(s3_silver_items_path)

#         # Write Data to Snowflake
#         write_to_snowflake(orders_df, "ORDERS", "target.order_id = source.order_id")
#         write_to_snowflake(order_items_df, "ORDER_ITEMS", 
#                            "target.order_id = source.order_id AND COALESCE(target.item_id, '') = COALESCE(source.item_id, '')")

#     except Exception as e:
#         logger.error(f"Error writing to Snowflake: {e}")
#     finally:
#         spark.stop()
# spark-submit   --master local[*] 
#   --packages org.apache.hadoop:hadoop-aws:3.3.6,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3,net.snowflake:snowflake-jdbc:3.13.14 
#   --conf spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY   --conf spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_KEY   
# --conf spark.hadoop.fs.s3a.endpoint=s3.us-east-2.amazonaws.com   --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem  write_to_snowflake.py

# if __name__ == "__main__":
#     main()


from pyspark.sql import SparkSession
import os
import logging
import snowflake.connector
from dotenv import load_dotenv

os.environ["SNOWFLAKE_OCSP_FAIL_OPEN"] = "true"

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName("WriteToSnowflake") \
    .config("spark.jars.packages", 
            "net.snowflake:snowflake-jdbc:3.13.14,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3") \
    .getOrCreate()

SF_OPTIONS = {
    "sfURL": "https://fg00255.switzerland-north.azure.snowflakecomputing.com",
    "sfDatabase": "DELIVERY",
    "sfSchema": "SILVER",
    "sfWarehouse": "COMPUTE_WH",
    "sfUser": os.getenv("SNOWFLAKE_USER"),
    "sfPassword": os.getenv("SNOWFLAKE_PASSWORD")
}

def execute_snowflake_query(queries, return_results=False):
    """Execute SQL queries using Snowflake Connector."""
    try:
        conn = snowflake.connector.connect(
            user=SF_OPTIONS["sfUser"],
            password=SF_OPTIONS["sfPassword"],
            account="fg00255.switzerland-north.azure"
        )
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {SF_OPTIONS['sfDatabase']};")
        cursor.execute(f"USE SCHEMA {SF_OPTIONS['sfSchema']};")
        
        results = []
        if isinstance(queries, str):
            queries = [queries]
            
        for query in queries:
            cursor.execute(query)
            if return_results:
                results.append(cursor.fetchall())
            conn.commit()
            
        cursor.close()
        conn.close()
        return results if return_results else None
    except Exception as e:
        logger.error(f"Error executing Snowflake query: {e}")
        raise

def write_to_snowflake(df, table_name):
    """Write DataFrame to Snowflake staging table and merge into the main table."""
    if df is None or df.isEmpty():
        logger.info(f"No data to write for {table_name}. Skipping...")
        return

    temp_table = f"{table_name}_STAGING"

    desc_query = f"DESC TABLE {table_name}"
    target_columns = execute_snowflake_query(desc_query, return_results=True)[0]
    target_column_names = [col[0].lower() for col in target_columns]
    
    df_aligned = df.select(target_column_names)

    setup_queries = [
        f"USE DATABASE {SF_OPTIONS['sfDatabase']};",
        f"USE SCHEMA {SF_OPTIONS['sfSchema']};",
        f"CREATE OR REPLACE TABLE {temp_table} LIKE {table_name};",
        f"TRUNCATE TABLE {temp_table};"
    ]
    execute_snowflake_query(setup_queries)

    df_aligned.write \
        .format("snowflake") \
        .options(**SF_OPTIONS) \
        .option("dbtable", temp_table) \
        .mode("append") \
        .save()

    # Generate merge query with aligned columns
    primary_keys = ['order_id']
    if "item_id" in target_column_names:
        primary_keys.append("item_id")

    update_columns = [f"target.{col} = source.{col}" for col in target_column_names if col not in primary_keys]
    merge_query = f"""
    MERGE INTO {table_name} AS target 
    USING {temp_table} AS source
    ON {" AND ".join([f'target.{pk} = source.{pk}' for pk in primary_keys])}
    WHEN MATCHED THEN
        UPDATE SET {', '.join(update_columns)}
    WHEN NOT MATCHED THEN
        INSERT ({', '.join(target_column_names)})
        VALUES ({', '.join([f'source.{col}' for col in target_column_names])})
    """

    execute_snowflake_query([merge_query, f"DROP TABLE IF EXISTS {temp_table}"])

def main():
    try:
        s3_silver_orders_path = "s3a://numa-delivery/silver/food-delivery-orders/"
        orders_df = spark.read.parquet(s3_silver_orders_path)

        s3_silver_items_path = "s3a://numa-delivery/silver/food-delivery-order_items/"
        order_items_df = spark.read.parquet(s3_silver_items_path)

        # Write Data to Snowflake
        write_to_snowflake(orders_df, "ORDERS")
        write_to_snowflake(order_items_df, "ORDER_ITEMS")

    except Exception as e:
        logger.error(f"Error writing to Snowflake: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

