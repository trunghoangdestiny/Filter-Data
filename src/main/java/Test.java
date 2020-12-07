import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.crypto.Data;

public class Test {
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        Logger.getLogger("org").setLevel(Level.OFF);

        String path = "/home/hoangquoctrung/Downloads/realData";

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("StreamData")
                .master("local")
                .getOrCreate();

        Dataset<Row> dataToMakeSchema = sparkSession
                .read()
                .parquet(path+"/parquet_logfile_at_00h_00.snap");

//        dataToMakeSchema.printSchema();
//        dataToMakeSchema.show(10);

        Dataset<Row> pc = dataToMakeSchema
                .filter(dataToMakeSchema.col("type").equalTo(1))
                .select("type", "location")
                .groupBy("location")
                .count()
                .withColumnRenamed("count", "pc");
//                .orderBy(dataToMakeSchema.col("location").asc());
        Dataset<Row> mb = dataToMakeSchema
                .filter(dataToMakeSchema.col("type").equalTo(3))
                .select("type", "location")
                .groupBy("location")
                .count()
                .withColumnRenamed("count", "mb");
//                .orderBy(dataToMakeSchema.col("location").asc());

        Dataset<Row> result = pc.join(mb, "location").orderBy("location");
        result.show(20);

        long end = System.currentTimeMillis();
        System.out.printf("Time: %d ms", end - start);
    }
}
