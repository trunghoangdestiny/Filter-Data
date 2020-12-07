import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import javax.xml.crypto.Data;

public class FilterData {
    public static void main(String[] args) {
//        long start = System.currentTimeMillis();
        Logger.getLogger("org").setLevel(Level.OFF);

        String path = "/data/platform/ssp_logs/*/*/*.snap";

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Filter Data")
                .getOrCreate();

        Dataset<Row> dataToMakeSchema = sparkSession
                .read()
                .parquet(path);

//        dataToMakeSchema.printSchema();

        Dataset<Row> pc = dataToMakeSchema
                .filter(dataToMakeSchema.col("type").equalTo(1))
                .select("type", "location")
                .groupBy("location")
                .count()
                .withColumnRenamed("count", "pc");
        Dataset<Row> mb = dataToMakeSchema
                .filter(dataToMakeSchema.col("type").equalTo(3))
                .select("type", "location")
                .groupBy("location")
                .count()
                .withColumnRenamed("count", "mb");

        Dataset<Row> rs = pc.join(mb, "location").orderBy("location");

        rs.show(5);

        rs
                .repartition(1)
                .write()
                .format("csv")
                .option("header", "true")
//                .option("encoding", "UTF-8")
                .save("/user/trunghv/lastResultAllDay");

        sparkSession.stop();

//        long end = System.currentTimeMillis();
//        System.out.printf("Time: %d ms", end - start);
    }
}
