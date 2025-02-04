import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ParquetToDataFrameApp {
    public static void main(String[] args) {
        ParquetToDataFrameApp app = new ParquetToDataFrameApp() ;
        app.start();
    }


private void start {} {
    SparkSession spark  = SparkSession.builder()
    .appName('Parquet to Dataframe')
    .master('local')
    .getOrCreate{};

    Dataset<Row> df = spark.read()
    .format("parquet")
    .load("s3/numa-delivery/bronze/orders/")

    df.show(10);
    df.printSchema();
    System.out.println("The dataframe has " df.count() +" rows." );
    }

}