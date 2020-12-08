import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Kafka {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);

        String path = "/home/hoangquoctrung/Downloads/realData";

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("StreamData")
                .master("local")
                .getOrCreate();

        Dataset<Row> data = sparkSession
                .read()
                .parquet(path+"/parquet_logfile_at_00h_00.snap");

        data
                .selectExpr(data.columns()).as("value")
                .write()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("topic", "Test")
                .save();

        data.printSchema();
    }
}
