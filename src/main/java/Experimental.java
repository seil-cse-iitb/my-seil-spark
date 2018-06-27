
import handler.ConfigHandler;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;


import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.Date;
import java.sql.Timestamp;


public class Experimental {




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

    public static Dataset<Row> getRowsByTableName(SparkSession sparkSession,String tableName) {
        Properties properties = getProperties();
        Dataset<Row> rows = sparkSession.read().jdbc(ConfigHandler.MYSQL_URL, tableName, properties);
        return rows;
    }


    public static Dataset<Row> QueryFunc(String room, long TS1, long TS2, Dataset<Row> sch_3 )

    {

        sch_3 = sch_3.where("TS>="+TS1+" and TS<="+TS2 + " and sensor_id="+room);
        return sch_3;

    }


    public static void main(String args[])
    {

       /* String room="\"power_k_erts_a\"", table_name="sch_3";
        logsOff();
        SparkSession sparkSession = SparkSession.builder().appName("Java Spark App")
                .config("spark.sql.warehouse.dir", "~/spark-warehouse")
                .config("spark.executor.memory", "2g")
                .config("spark.driver.allowMultipleContexts", "true")
                .master("local[4]")
                .getOrCreate();

        Dataset<Row> sch_3 = getRowsByTableName(sparkSession, table_name);

        Date now = new Date();
        Timestamp current = new Timestamp(now.getTime());

        long TS1 = now.getTime();
        System.out.println(TS1);
        TS1=TS1/1000 - 604800; //week
        TS1=TS1/1000 - 31536000; //year
        TS1=TS1-(TS1%60);
        long TS2=TS1+7200;

        sch_3=QueryFunc(room,TS1,TS2,sch_3);*/









    }
}
