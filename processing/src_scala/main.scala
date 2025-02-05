import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object FoodDeliveryETL {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FoodDeliveryETL")
      .config("spark.jars.packages", 
              "org.apache.hadoop:hadoop-aws:3.3.6,net.snowflake:spark-snowflake_2.12:3.1.0")
      .config("spark.hadoop.fs.s3a.access.key", sys.env("AWS_ACCESS_KEY"))
      .config("spark.hadoop.fs.s3a.secret.key", sys.env("AWS_SECRET_KEY"))
      .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
      .getOrCreate()

    val baseS3BronzePath = "s3a://numa-delivery/bronze/food-delivery-orders-raw"
    val s3SilverOrdersPath = "s3a://numa-delivery/silver/food-delivery-orders/"

    getDatesInBronze().foreach { date =>
      val s3BronzePath = s"$baseS3BronzePath/$date/*"
      val bronzeDF = spark.read.parquet(s3BronzePath)
      val silverDF = transformToSilver(bronzeDF)
      silverDF.write.mode("append").parquet(s3SilverOrdersPath)
      writeToSnowflake(silverDF, "ORDER")
    }

    spark.stop()
  }

  def transformToSilver(df: DataFrame): DataFrame = {
    df.withColumn("event_timestamp", to_timestamp(col("event_timestamp"), "yyyy-MM-dd HH:mm:ss.SSSSSS"))
      .withColumn("total_amount", col("total_amount").cast("double"))
      .filter(col("order_id").isNotNull && col("merchant_id").isNotNull && col("total_amount") > 0)
  }

  def writeToSnowflake(df: DataFrame, tableName: String): Unit = {
    df.write
      .format("net.snowflake.spark.snowflake")
      .options(Map(
        "sfURL" -> "https://fg00255.switzerland-north.azure.snowflakecomputing.com",
        "sfUser" -> sys.env("SNOWFLAKE_USER"),
        "sfPassword" -> sys.env("SNOWFLAKE_PASSWORD"),
        "sfDatabase" -> "DELIVERY",
        "sfSchema" -> "SILVER",
        "sfWarehouse" -> "COMPUTE_WH"
      ))
      .option("dbtable", tableName)
      .mode("append")
      .save()
  }

  def getDatesInBronze(): List[String] = {
    val formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    val startDate = LocalDate.of(2025, 2, 1)
    val endDate = LocalDate.now()
    Iterator.iterate(startDate)(_ plusDays 1).takeWhile(!_.isAfter(endDate)).map(_.format(formatter)).toList
  }
}
