from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    count,
    sum,
    when,
    hour,
    abs,
    round,
)
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = (
    SparkSession.builder.appName("GOLDLAYER")
    .config(
        "spark.jars.packages",
        "net.snowflake:snowflake-jdbc:3.13.14,net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3",
    )
    .getOrCreate()
)

SF_OPTIONS = {
    "sfURL": "https://fg00255.switzerland-north.azure.snowflakecomputing.com",
    "sfDatabase": "DELIVERY",
    "sfSchema": "SILVER",
    "sfWarehouse": "COMPUTE_WH",
    "sfUser": os.getenv("SNOWFLAKE_USER"),
    "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
}


def calculate_fulfillment_metrics(orders_df):
    """Calculate order fulfillment rates and related metrics"""
    return orders_df.groupBy("event_date").agg(
        count("order_id").alias("total_orders"),
        count(when(col("order_status") == "DELIVERED", True)).alias("delivered_orders"),
        round(
            count(when(col("order_status") == "DELIVERED", True))
            / count("order_id")
            * 100,
            2,
        ).alias("fulfillment_rate"),
        count(when(col("order_status") == "CANCELLED", True)).alias("cancelled_orders"),
        round(
            count(when(col("order_status") == "CANCELLED", True))
            / count("order_id")
            * 100,
            2,
        ).alias("cancellation_rate"),
        round(avg(when(col("is_delayed") == True, 1).otherwise(0)) * 100, 2).alias(
            "delay_rate"
        ),
    )


def calculate_delivery_time_metrics(orders_df):
    """Calculate delivery time metrics"""
    return (
        orders_df.filter(col("order_status") == "DELIVERED")
        .groupBy("event_date")
        .agg(
            round(avg("delivery_time_minutes"), 2).alias("avg_delivery_time"),
            round(
                avg(when(col("is_delayed") == True, col("delivery_time_minutes"))), 2
            ).alias("avg_delayed_delivery_time"),
            round(
                avg(when(col("is_delayed") == False, col("delivery_time_minutes"))), 2
            ).alias("avg_ontime_delivery_time"),
        )
    )


def calculate_order_volume_metrics(orders_df, order_items_df):
    """Calculate order volume and revenue metrics"""
    # Join orders with items for revenue calculations
    order_metrics = (
        orders_df.join(order_items_df, "order_id", "left")
        .groupBy("event_date")
        .agg(
            count("distinct order_id").alias("total_orders"),
            round(sum("total_amount"), 2).alias("total_revenue"),
            round(avg("total_amount"), 2).alias("avg_order_value"),
            count("distinct merchant_id").alias("active_merchants"),
            count("distinct customer_id").alias("active_customers"),
        )
    )

    # Add hourly distribution
    hourly_distribution = (
        orders_df.groupBy("event_date", hour("event_timestamp").alias("hour"))
        .count()
        .groupBy("event_date")
        .agg(
            count("*").alias("active_hours"),
            round(avg("count"), 2).alias("avg_orders_per_hour"),
        )
    )

    return order_metrics.join(hourly_distribution, "event_date")


def calculate_eta_accuracy_metrics(orders_df):
    """Calculate ETA accuracy metrics"""
    return (
        orders_df.filter(col("order_status") == "DELIVERED")
        .groupBy("event_date")
        .agg(
            round(
                avg(
                    abs(
                        col("delivery_time_minutes")
                        - col("estimated_delivery_time").cast("timestamp")
                    )
                ),
                2,
            ).alias("avg_eta_deviation"),
            count(when(col("is_delayed") == True, True)).alias("delayed_deliveries"),
            round(avg(when(col("is_delayed") == True, 1).otherwise(0)) * 100, 2).alias(
                "delay_rate"
            ),
        )
    )


def write_metrics_to_gold(metrics_df, table_name):
    """Write metrics to gold schema"""
    gold_options = SF_OPTIONS.copy()
    gold_options["sfSchema"] = "GOLD"  # Update schema to GOLD

    try:
        metrics_df.write.format("snowflake").options(**gold_options).option(
            "dbtable", table_name
        ).mode("append").save()
        logger.info(f"Successfully wrote metrics to GOLD.{table_name}")
    except Exception as e:
        logger.error(f"Error writing to GOLD.{table_name}: {e}")
        raise e


def main():
    orders_df = spark.read \
        .format("snowflake") \
        .options(**SF_OPTIONS) \
        .option("dbtable", "ORDERS") \
        .load()
    

    order_items_df = (
        spark.read.format("snowflake")
        .options(**SF_OPTIONS)
        .option("dbtable", "ORDER_ITEMS")
        .load()
   

    # Calculate metrics
    try:
        # Fulfillment metrics
        fulfillment_metrics = calculate_fulfillment_metrics(orders_df)
        write_metrics_to_gold(fulfillment_metrics, "FULFILLMENT_METRICS")

        # Delivery time metrics
        delivery_metrics = calculate_delivery_time_metrics(orders_df)
        write_metrics_to_gold(delivery_metrics, "DELIVERY_TIME_METRICS")

        # Order volume metrics
        volume_metrics = calculate_order_volume_metrics(orders_df, order_items_df)
        write_metrics_to_gold(volume_metrics, "ORDER_VOLUME_METRICS")

        # ETA accuracy metrics
        eta_metrics = calculate_eta_accuracy_metrics(orders_df)
        write_metrics_to_gold(eta_metrics, "ETA_ACCURACY_METRICS")

    except Exception as e:
        logger.error(f"Error in metrics calculation: {e}")
        raise e
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
