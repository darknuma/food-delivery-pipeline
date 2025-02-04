import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;

import java.util.HashMap;
import java.util.Map;

public class SilverLayerProcessingApp {
    public static void main(String[] args) {
        SilverLayerProcessingApp app = new SilverLayerProcessingApp();
        app.start();
    }

    private void start() {
        // Initialize SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Silver Layer Processing")
                .master("local[*]") // Use local mode for testing
                .getOrCreate();

        String s3Path = "s3a://numa-delivery/bronze/orders/";

        // Read Parquet data from S3 (Bronze Layer)
        Dataset<Row> bronzeOrders = spark.read()
                .format("parquet")
                .load(s3Path);

        // Transform bronze data into silver format
        Dataset<Row> silverOrders = bronzeOrders
                .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
                .withColumn("estimated_delivery_time", to_timestamp(col("estimated_delivery_time")))
                .withColumn("items", from_json(col("items"), DataTypes.createArrayType(
                        DataTypes.createStructType(new StructField[]{
                                new StructField("item_id", DataTypes.StringType, false, Metadata.empty()),
                                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                                new StructField("quantity", DataTypes.IntegerType, false, Metadata.empty()),
                                new StructField("unit_price", DataTypes.createDecimalType(10, 2), false, Metadata.empty()),
                                new StructField("total_price", DataTypes.createDecimalType(10, 2), false, Metadata.empty())
                        })
                )))
                .withColumn("delivery_location", from_json(col("delivery_location"), DataTypes.createStructType(new StructField[]{
                        new StructField("latitude", DataTypes.DoubleType, false, Metadata.empty()),
                        new StructField("longitude", DataTypes.DoubleType, false, Metadata.empty()),
                        new StructField("address", DataTypes.StringType, false, Metadata.empty())
                })));

        Dataset<Row> flattenedOrders = silverOrders
                .withColumn("item", explode(col("items")))
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
                );

        Dataset<Row> aggregatedOrders = flattenedOrders
                .groupBy("order_id")
                .agg(
                        count("*").alias("total_items"),
                        sum("total_price").alias("total_order_value"),
                        min("event_timestamp").alias("order_time"),
                        max("estimated_delivery_time").alias("max_estimated_delivery")
                );

        aggregatedOrders.show(10);
        aggregatedOrders.printSchema();

        Map<String, String> snowflakeOptions = new HashMap<>();
        snowflakeOptions.put("sfURL", "https://your_snowflake_account.snowflakecomputing.com");
        snowflakeOptions.put("sfUser", "YOUR_USERNAME");
        snowflakeOptions.put("sfPassword", "YOUR_PASSWORD");
        snowflakeOptions.put("sfDatabase", "YOUR_DATABASE");
        snowflakeOptions.put("sfSchema", "YOUR_SCHEMA");
        snowflakeOptions.put("sfWarehouse", "YOUR_WAREHOUSE");
        snowflakeOptions.put("dbtable", "SILVER_ORDERS");

        aggregatedOrders.write()
                .format("snowflake")
                .options(snowflakeOptions)
                .mode("append")
                .save();

        spark.stop();
    }
}
