import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.functions.*;
import java.util.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class FoodDeliveryETL {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("FoodDeliveryETL")
                .config("spark.jars.packages", 
                        "org.apache.hadoop:hadoop-aws:3.3.6,net.snowflake:spark-snowflake_2.12:3.1.0")
                .config("spark.hadoop.fs.s3a.access.key", System.getenv("AWS_ACCESS_KEY"))
                .config("spark.hadoop.fs.s3a.secret.key", System.getenv("AWS_SECRET_KEY"))
                .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
                .getOrCreate();

        List<String> dateList = getDatesInBronze();
        String baseS3BronzePath = "s3a://numa-delivery/bronze/food-delivery-orders-raw";
        String s3SilverOrdersPath = "s3a://numa-delivery/silver/food-delivery-orders/";

        for (String date : dateList) {
            String s3BronzePath = baseS3BronzePath + "/" + date + "/*";

            Dataset<Row> bronzeDF = spark.read()
                    .parquet(s3BronzePath);

            Dataset<Row> silverDF = transformToSilver(bronzeDF);
            silverDF.write().mode("append").parquet(s3SilverOrdersPath);

            writeToSnowflake(silverDF, "ORDER");
        }

        spark.stop();
    }

    public static Dataset<Row> transformToSilver(Dataset<Row> df) {
        return df.withColumn("event_timestamp", functions.to_timestamp(df.col("event_timestamp"), "yyyy-MM-dd HH:mm:ss.SSSSSS"))
                .withColumn("total_amount", df.col("total_amount").cast("double"))
                .filter(df.col("order_id").isNotNull().and(df.col("merchant_id").isNotNull()).and(df.col("total_amount").gt(0)));
    }

    public static void writeToSnowflake(Dataset<Row> df, String tableName) {
        Map<String, String> sfOptions = new HashMap<>();
        sfOptions.put("sfURL", "https://fg00255.switzerland-north.azure.snowflakecomputing.com");
        sfOptions.put("sfUser", System.getenv("SNOWFLAKE_USER"));
        sfOptions.put("sfPassword", System.getenv("SNOWFLAKE_PASSWORD"));
        sfOptions.put("sfDatabase", "DELIVERY");
        sfOptions.put("sfSchema", "SILVER");
        sfOptions.put("sfWarehouse", "COMPUTE_WH");

        df.write().format("net.snowflake.spark.snowflake")
                .options(sfOptions)
                .option("dbtable", tableName)
                .mode("append")
                .save();
    }

    public static List<String> getDatesInBronze() {
        List<String> dateList = new ArrayList<>();
        LocalDate startDate = LocalDate.of(2025, 2, 1);
        LocalDate endDate = LocalDate.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd");

        while (!startDate.isAfter(endDate)) {
            dateList.add(startDate.format(formatter));
            startDate = startDate.plusDays(1);
        }
        return dateList;
    }
}
