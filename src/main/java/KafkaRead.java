
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import scala.Int;

import java.util.Arrays;
import java.util.Iterator;

import static WithMqtt.PrevYearJoinWithCurrentSlot.logsOff;
import static org.apache.spark.sql.types.DataTypes.TimestampType;


public class KafkaRead {


    public static void main(String args[]) throws Exception
    {
        logsOff();

        SparkConf sparkConf = new SparkConf().setAppName("kafkaRead");


        String key="data/kresit/sch/16";
        String topic="data";
        // check Spark configuration for master URL, set it to local if not configured
        if (!sparkConf.contains("spark.master")) {
            sparkConf.setMaster("local[4]");
        }

        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

      /*  Dataset <Row> streamdf = spark
                            .readStream()
                            .format("kafka")
                            .option("kafka.bootstrap.servers", "10.129.149.18:9092,10.129.149.19:9092,10.129.149.20:9092")
                            .option("subscribe", topic)
                            .option("startingOffsets", "earliest")

                            .load();

        streamdf=streamdf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
        streamdf=streamdf.where("key = \"data/kresit/sch/16\" ");


        SQLContext sqlContext= new SQLContext(spark);
        // Generate running word count
        streamdf.createOrReplaceTempView("kdata");
        String sql="select value from kdata";
        Dataset<Row> powerData = sqlContext.sql(sql);
        //powerData.printSchema();

        Dataset<Row> newData=powerData.selectExpr("split(value, ',')[0] as srl",
                "split(value, ',')[1] as timestamp",
                "split(value, ',')[2] as VA",
                "split(value, ',')[3] as W",
                "split(value, ',')[4] as VAR",
                "split(value, ',')[5] as PF",
                "split(value, ',')[6] as VLL",
                "split(value, ',')[7] as VLN",
                "split(value, ',')[8] as A",
                "split(value, ',')[9] as F",
                "split(value, ',')[10] as VA1",
                "split(value, ',')[11] as W1",
                "split(value, ',')[12] as VAR1",
                "split(value, ',')[13] as PF1",
                "split(value, ',')[14] as V12",
                "split(value, ',')[15] as V1",
                "split(value, ',')[16] as A1",
                "split(value, ',')[17] as VA2",
                "split(value, ',')[18] as W2",
                "split(value, ',')[19] as VAR2",
                "split(value, ',')[20] as PF2",
                "split(value, ',')[21] as V23",
                "split(value, ',')[22] as V2",
                "split(value, ',')[23] as A2",
                "split(value, ',')[24] as VA3",
                "split(value, ',')[25] as W3",
                "split(value, ',')[26] as VAR3",
                "split(value, ',')[27] as PF3",
                "split(value, ',')[28] as V31",
                "split(value, ',')[29] as V3",
                "split(value, ',')[30] as A3",
                "split(value, ',')[31] as FwdVAh",
                "split(value, ',')[32] as FwdWh",
                "split(value, ',')[33] as FwdVARhR",
                "split(value, ',')[34] as FwdVARhC");
        newData.createOrReplaceTempView("knewdata");
        String query="select * from knewdata";
        Dataset<Row> finalData = sqlContext.sql(query);
        finalData.printSchema();


        finalData= finalData.withColumn("time", functions.current_timestamp());
        StreamingQuery streamQuery =
                finalData
                        .writeStream()
                        .option("numRows",3000)

                        .trigger(Trigger.ProcessingTime(60000))
                        .format("console")
                        .option("truncate", false).start();

        streamQuery.awaitTermination();*/

        Dataset <Row> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.129.149.18:9092,10.129.149.19:9092,10.129.149.20:9092")
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")

                .load();

        lines=lines.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
        lines=lines.where("key = \"data/kresit/sch/16\" ");


        SQLContext sqlContext= new SQLContext(spark);
        // Generate running word count
//        lines.createOrReplaceTempView("data");
        Dataset<Row> powerData = lines.select("value");
        //powerData.printSchema();

        Dataset<Row> newData=powerData.selectExpr("split(value, ',')[0] as srl",
                "CAST(split(value, ',')[1] AS LONG) as timestamp",
                "split(value, ',')[2] as VA",
                "CAST(split(value, ',')[3] AS DOUBLE) as W",
                "split(value, ',')[4] as VAR",
                "split(value, ',')[5] as PF",
                "split(value, ',')[6] as VLL",
                "split(value, ',')[7] as VLN",
                "split(value, ',')[8] as A",
                "split(value, ',')[9] as F",
                "split(value, ',')[10] as VA1",
                "split(value, ',')[11] as W1",
                "split(value, ',')[12] as VAR1",
                "split(value, ',')[13] as PF1",
                "split(value, ',')[14] as V12",
                "split(value, ',')[15] as V1",
                "split(value, ',')[16] as A1",
                "split(value, ',')[17] as VA2",
                "split(value, ',')[18] as W2",
                "split(value, ',')[19] as VAR2",
                "split(value, ',')[20] as PF2",
                "split(value, ',')[21] as V23",
                "split(value, ',')[22] as V2",
                "split(value, ',')[23] as A2",
                "split(value, ',')[24] as VA3",
                "split(value, ',')[25] as W3",
                "split(value, ',')[26] as VAR3",
                "split(value, ',')[27] as PF3",
                "split(value, ',')[28] as V31",
                "split(value, ',')[29] as V3",
                "split(value, ',')[30] as A3",
                "split(value, ',')[31] as FwdVAh",
                "split(value, ',')[32] as FwdWh",
                "split(value, ',')[33] as FwdVARhR",
                "split(value, ',')[34] as FwdVARhC");

        newData=newData.select("timestamp","W");
        Dataset<Row> newData1=newData.select("timestamp","W");
        newData = newData.withColumn("secondsYear", functions.lit(86400));
        Column sToDate= newData.col("timestamp");
        Column nums= newData.col("secondsYear");

        Column c= sToDate.minus(nums).cast("bigint");
        newData=newData.withColumn("sToDate",c);


        Column timestamp1 = functions.col("timestamp").cast(TimestampType).as("eventTime");

        Dataset<Row> resu =newData.groupBy(functions.window(timestamp1,"1 hour","15 minute")).avg("W").orderBy("window");

        Column windowstart_s = resu.col("window.start").cast("timestamp").cast("long");
        resu=resu.withColumn("windowstart_s",windowstart_s);
        resu = resu.withColumn("secondsYear", functions.lit(86400));
        nums= resu.col("secondsYear");

        Column cr= windowstart_s.minus(nums).cast("bigint");
        resu=resu.withColumn("sToDate",cr);
        resu=resu.withColumn("WindowStart", functions.from_unixtime(functions.col("sToDate").minus(functions.col("sToDate").mod(60))));

//        resu=resu.join(res,"WindowStart");
        StreamingQuery streamQuery =
                resu
                        .writeStream()
                        .option("numRows",3000)

                        .outputMode("complete")
                        .trigger(Trigger.ProcessingTime(6000))
                        .format("console")
                        .option("truncate", false).start();

        streamQuery.awaitTermination();

    }
}
