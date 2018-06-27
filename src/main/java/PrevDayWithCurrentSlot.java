//PowerConsumption

import handler.ConfigHandler;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import static org.apache.spark.sql.types.DataTypes.TimestampType;

public class PrevDayWithCurrentSlot {



    public static void logsOff() {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
    }

    public static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("user", ConfigHandler.MYSQL_USERNAME);
        properties.setProperty("password", ConfigHandler.MYSQL_PASSWORD);
        return properties;
    }

    public static Dataset<Row> getRowsByTableName(SparkSession sparkSession, String tableName) {
        Properties properties = getProperties();
        Dataset<Row> rows = sparkSession.read().jdbc(ConfigHandler.MYSQL_URL, tableName, properties);
        return rows;
    }

    public static Dataset<String> Stream(String topic,String brokerUrl)
    {


        SparkConf sparkConf = new SparkConf().setAppName("PrevDayWithCurrentSlot");



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
        return lines;
    }

    public static Dataset<Row> FilterRows(SparkSession sparkSession,String room, long TS1, long TS2, String tableName )
    {


        Dataset<Row> sch_3 = getRowsByTableName(sparkSession, tableName);
        sch_3 = sch_3.where("TS>="+TS1+" and TS<="+TS2 + " and sensor_id="+room);
        return sch_3;

    }
    public static Dataset<Row> FilterRowsPrevDay(SparkSession sparkSession,String room, String tableName , long timeInterval)
    {


        Date now = new Date();
        Timestamp current = new Timestamp(now.getTime());

        long TS1 = now.getTime();
        System.out.println(TS1);
        TS1=TS1/1000 - 86400; //week
      //  TS1=TS1/1000 - 31536000; //year
        TS1=TS1-(TS1%60);
        long TS2=TS1+timeInterval;

        Dataset<Row> sch_3 = getRowsByTableName(sparkSession, tableName);
        sch_3 = sch_3.where("TS>="+TS1+" and TS<="+TS2 + " and sensor_id="+room);
        return sch_3;

    }

    public static void main(String args[]) throws Exception
    {

        String room="\"power_k_sr_a\"";
        logsOff();
        SparkSession sparkSession = SparkSession.builder().appName("Java Spark App")
                .config("spark.sql.warehouse.dir", "~/spark-warehouse")
                .config("spark.executor.memory", "2g")
                .config("spark.driver.allowMultipleContexts", "true")
                .master("local[4]")
                .getOrCreate();

        Dataset<Row> sch_3 = getRowsByTableName(sparkSession, "sch_3");


        Date now = new Date();


        long tm = System.currentTimeMillis();
        long epoch = now.getTime();
        System.out.println(epoch);
        epoch=epoch/1000 - 86400;
        epoch=epoch-(epoch%60);
//        long epoch2=epoch+21600;
        long epoch2=epoch+86400;

        // 1495695343 1495695360
        //epoch=1495622280;
        //epoch2=1495622280+60;
        epoch=epoch;
        // epoch=1495693980;
        // epoch2=1495698000;
        System.out.println(epoch+" "+epoch2); // 12-1 pm
        sch_3 = sch_3.where("TS>="+epoch+" and TS<="+epoch2 + " and sensor_id="+room);
//        sch_3.show();



        long tm1 = System.currentTimeMillis();

        System.out.println("TIME taken to fetch from db "+ (tm1-tm));
        System.out.print(sch_3.queryExecution().simpleString());
        Column timestamp = functions.col("TS").cast(TimestampType).as("eventTime");


//        Dataset<Row> qwe= sch_3.where("TS>="+1495690380+" and TS<="+ 1495693980); //11-12pm
//       // qwe.groupBy(functions.col("sensor_id"),functions.window(timestamp,"1 hour","1 minute")).avg("W").sort("window").show(1000,false);
//        qwe.show(3000,false);
//
//        System.out.println("\nhere "+ qwe.count());

//		sch_3.select(functions.window(timestamp,"1 hour","1 minute")).printSchema();
        Dataset<Row> result =sch_3.groupBy(functions.col("sensor_id"),functions.window(timestamp,"1 hour","15 minute")).avg("W").sort("window");
//        result.show();



        Column windowstart = result.col("window.start");
        Column windowend = result.col("window.end");
        // result.withColumn("WindowStart", windowstart).show(12000,false);
        result.printSchema();
        Date date=new Date(epoch*1000), date2=new Date(epoch2*1000);
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String formatted = format.format(date),formatted2=format.format(date2);
        System.out.println(formatted);
        //Dataset<Row> res= result.where("window.start>=\"2017-06-13 13:59:00.0\"");
        Dataset<Row> res= result.where("window.start>=\""+formatted + "\" and " + "window.end<=\""+formatted2+"\"");
        res =res.withColumn("WindowStart", windowstart).withColumnRenamed("avg(W)", "Archival Average W");
        res =res.withColumn("WindowEnd", windowend);
//        res.show();

        System.out.println(res.count());


        tm1 = System.currentTimeMillis();

        System.out.println("TIME taken to fetch from db + windowing hourly"+ (tm1-tm));









        //STREAM

        String brokerUrl = "tcp://10.129.149.9:1883";
        String topic = "data/kresit/sch/12";

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

        resu=resu.join(res,"WindowStart");
        StreamingQuery streamQuery =
                resu
                        .writeStream()
                        .option("numRows",3000)

                        .outputMode("complete")
                        .trigger(Trigger.ProcessingTime(900000))
                        .format("console")
                        .option("truncate", false).start();

        streamQuery.awaitTermination();


    }

}
