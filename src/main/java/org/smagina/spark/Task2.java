package org.smagina.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.apache.spark.sql.functions.sum;

public class Task2 {

    private static String HADOOP_URL = "hdfs://localhost:9000/";
    /*
     spark-submit --master local[*]  --packages org.apache.spark:spark-avro_2.11:2.4.3 --class org.smagina.spark.Task2 /mnt/c/Users/helen/IdeaProjects/spark-task/target/spark-task2-1.0-SNAPSHOT-jar-with-dependencies.jar
     */

    public static void main(String[] args) throws StreamingQueryException {
        System.out.println("Start spark task2 ....");

        SparkSession spark = SparkSession.builder()
                .appName("Task2 Streaming App")
                .config("spark.sql.streaming.schemaInference", true) //Use schema extracted from source file
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        System.out.println(String.format("Spark session crated (version %s)", spark.version()));

        System.out.println("Start calculation of hotel's statistic for 2016");
        // Read Expedia Data for 2016
        Dataset<Row> df_expedia = getExpediaFor2016(spark);
        df_expedia.printSchema();
        //Read enriched hotel data by weather from Kafka topic
        Dataset<Row> ds_weather = spark.read()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9094")
                .option("subscribe", "weather_plus_hotel")
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .load()
                .selectExpr("CAST(value AS STRING)") //read binary format from Kafka as a string
                .selectExpr("select from_json(*)")
                .where("avg_temp > 0"); //Filter incoming data by having average temperature more than 0 Celsius degrees
        // weather data joined to expedia
        df_expedia = calculateHotelStatistic(df_expedia.join(ds_weather, df_expedia.col("hotel_id").equalTo(ds_weather.col("hotel_id"))));
        /*
        Store it as initial state (For examples: hotel, batch_timestamp, erroneous_data_cnt,
        short_stay_cnt, standart_stay_cnt, standart_extended_stay_cnt, long_stay_cnt, most_popular_stay_type).
         */
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        Broadcast<List<Row>> initialStateBroadcast = javaSparkContext.broadcast(df_expedia.collectAsList());
        System.out.println("Initial state stored as broadcast variable with size: " + initialStateBroadcast.value().size());

        StructType initialStateSchema = df_expedia.schema();
        System.out.println("Finish calculation of hotel's statistic for 2016");

        System.out.println("Start calculation of hotel's statistic for 2017");
        // In streaming way read Expedia data for 2017 year from HDFS on WSL2.
        Dataset<Row> df_expedia_stream = getExpediaStreamFor2017(spark);

        StreamingQuery sq = df_expedia_stream
                .writeStream()
                .outputMode(OutputMode.Append())
                .foreachBatch((dataset, batchId) -> {
                    // Read initial state, send it via broadcast into streaming
                    Dataset<Row> initialState = dataset.sparkSession().createDataFrame(initialStateBroadcast.value(), initialStateSchema);
                    dataset.sparkSession().sparkContext();
                    // Repeat previous logic on the stream (calculate hotel statistic)
                    calculateHotelStatistic(dataset)
                            .union(initialState)
                            // Store final data in HDFS. (Result will look like: hotel, with_children, batch_timestamp, erroneous_data_cnt,
                            // short_stay_cnt, standart_stay_cnt, standart_extended_stay_cnt, long_stay_cnt, most_popular_stay_type
                            .write()
                            .mode(SaveMode.Append)
                            .format("avro")
                            .save(HADOOP_URL + "expedia_stream_result");
                })
                .start();

        spark.streams().addListener(new StreamListenerImpl(sq));

        sq.awaitTermination();

        // check result
        getStreamResult(spark).select("batch_timestamp").groupBy("batch_timestamp").count().show();

        System.out.println("Finish calculation of hotel's statistic for 2017");

        //timeout to analysis
        try {
            Thread.sleep(10000000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Finish spark task2 ....");
    }

    private static Dataset<Row> getStreamResult(SparkSession spark) {
        return spark.read().format("avro").load(HADOOP_URL + "expedia_stream_result");
    }

    private static Dataset<Row> getExpediaFor2016(SparkSession spark) {
        return spark.read().format("avro").load(HADOOP_URL + "expedia")
                .selectExpr("id", "hotel_id", "srch_ci", "srch_co")
                .where("substring(srch_ci,1,4) == 2016");
    }

    private static Dataset<Row> getExpediaStreamFor2017(SparkSession spark) {
        return spark.readStream()
                .option("maxFilesPerTrigger", 7) //Read max 7 files for one microbatch
                .option("spark.sql.streaming.schemaInference", "true") //Read schema from avro file
                .format("avro")
                .load(HADOOP_URL + "expedia")
                .selectExpr("id", "hotel_id", "srch_ci", "srch_co")
                .where("substring(srch_ci,1,4) == 2017");
    }

    /*
    •	Calculate customer's duration of stay as days between requested check-in and check-out date.
    •	Create customer preferences of stay time based on next logic.
    -	Map each hotel with multi-dimensional state consisting of record counts for each type of stay:
        	"Erroneous data": null, more than month(30 days), less than or equal to 0
        	"Short stay": 1 day stay
        	"Standart stay": 2-7 days
        	"Standart extended stay": 1-2 weeks
          	"Long stay": 2-4 weeks (less than month)
     */
    public static Dataset<Row> calculateHotelStatistic(Dataset<Row> dataset) {
        String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        return dataset
                .selectExpr("*", "datediff(srch_co,srch_ci) as days")
                .selectExpr(
                        "*",
                        String.format("'%s' as batch_timestamp", timeStamp),
                        "case when days is NULL or days > 30 or days <=0 then 1 else 0 end as erroneous_data",
                        "case when days == 1 then 1 else 0 end as short_stay",
                        "case when days > 1 and days <= 7 then 1 else 0 end as standart_stay",
                        "case when days > 7 and days <= 14 then 1 else 0 end as standart_extended_stay",
                        "case when days > 14 and days <= 30 then 1 else 0 end as long_stay"
                )
                .groupBy("hotel_id", "batch_timestamp").agg(
                        sum("erroneous_data").as("erroneous_data_cnt"),
                        sum("short_stay").as("short_stay_cnt"),
                        sum("standart_stay").as("standart_stay_cnt"),
                        sum("standart_extended_stay").as("standart_extended_stay_cnt"),
                        sum("long_stay").as("long_stay_cnt")
                );
    }
}
