
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import scala.Int;
import org.apache.spark.sql.SQLContext;

import java.util.Arrays;
import java.util.Iterator;

public class MqttRead {


    public static void main(String[] args) throws Exception {
       /* if (args.length < 2) {
            System.err.println("Usage: JavaMQTTStreamWordCount <brokerUrl> <topic>");
            System.exit(1);
        }*/

//        if (!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
//            Logger.getRootLogger().setLevel(Level.WARN);
//        }

        String brokerUrl = "tcp://10.129.149.9:1883";
        String topic = "data/kresit/sch/16";

        SparkConf sparkConf = new SparkConf().setAppName("mqttRead");

        // check Spark configuration for master URL, set it to local if not configured
        if (!sparkConf.contains("spark.master")) {
            sparkConf.setMaster("spark://10.129.149.14:7077");
        }

        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        // Create DataFrame representing the stream of input lines from connection to mqtt server
        Dataset<String> lines = spark
                .readStream()
                .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
                .option("topic", topic)
               // .option("clientID","TestStream")
                .load(brokerUrl).select("value").as(Encoders.STRING());

        // Split the lines into words

            lines.createOrReplaceTempView("updates");
            System.out.println("   THIS  " +spark.sql("select count(*) from updates"));

        /*StreamingQuery streamQuery=    lines.writeStream()
                .trigger(Trigger.ProcessingTime(20000))
                .format("console")
                .start();*/

       /* Dataset<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            public Iterator<String> call(String x) {
                return Arrays.asList(x.split(",")).iterator();
            }
        }, Encoders.STRING());*/

        SQLContext sqlContext= new SQLContext(spark);
        // Generate running word count
        lines.createOrReplaceTempView("data");
        String sql="select value from data";
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
        newData.createOrReplaceTempView("newdata");
        String query="select * from newdata";
        Dataset<Row> finalData = sqlContext.sql(query);
        finalData.printSchema();
        // finalData.explain();
        //System.out.println(finalData.columns().toString());

        StreamingQuery streamQuery =
                finalData
                        .writeStream()
                        .option("numRows",3000)

                      //  .trigger(Trigger.ProcessingTime(60000))
                        .format("console")
                        .option("truncate", false).start();

        streamQuery.awaitTermination();
    }

}
