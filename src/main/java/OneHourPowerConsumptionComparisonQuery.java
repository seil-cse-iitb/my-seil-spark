import handler.ConfigHandler;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.expressions.Window;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;

public class OneHourPowerConsumptionComparisonQuery {

	public static void logsOff() {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
	}

	public static Properties getProperties() {
		Properties properties = new Properties();
		properties.setProperty("user", ConfigHandler.MYSQL_USERNAME);
		properties.setProperty("password", ConfigHandler.MYSQL_PASSWORD);
		properties.setProperty("driver", "com.mysql.cj.jdbc.Driver");
		return properties;
	}

	public static Dataset<Row> getRowsByTableName(SparkSession sparkSession,String tableName) {
		Properties properties = getProperties();

		Dataset<Row> rows = sparkSession.read().jdbc(ConfigHandler.MYSQL_URL, tableName, properties);
		return rows;
	}
	public static void main(String[] args) {


	    String room="\"power_k_erts_a\"";
		logsOff();
		SparkSession sparkSession = SparkSession.builder().appName("Java Spark App")
				.config("spark.sql.warehouse.dir", "~/spark-warehouse")
				.config("spark.executor.memory", "2g")
				.config("spark.driver.allowMultipleContexts", "true")
				.master("spark://10.129.149.14:7077")
				.getOrCreate();

		Dataset<Row> sch_3 = getRowsByTableName(sparkSession, "sch_3");


		Date now = new Date();
//		Timestamp current = new Timestamp(now.getTime());

        long tm = System.currentTimeMillis();
		long epoch = now.getTime();
		System.out.println(epoch);
		epoch=epoch/1000 - 3024000;
//		epoch=epoch/1000 - 172000;
        epoch=epoch-(epoch%60);
		long epoch2=epoch+18000;

       // 1495695343 1495695360
		//epoch=1495622280;
		//epoch2=1495622280+60;
        epoch=epoch+30;
       // epoch=1495693980;
       // epoch2=1495698000;
        System.out.println(epoch+" "+epoch2); // 12-1 pm
		epoch=1519084800;
		epoch2=1521504000;
		sch_3 = sch_3.where("TS>="+epoch+" and TS<="+epoch2 + " and sensor_id="+room);
		sch_3.show(700000,false);

        long tm1 = System.currentTimeMillis();
        System.out.println("TIME FROM DB"+(tm1-tm));

        Column timestamp = functions.col("TS").cast(DataTypes.TimestampType).as("eventTime");


//        Dataset<Row> qwe= sch_3.where("TS>="+1519084800+" and TS<="+ 1521504000); //11-12pm
//        qwe.groupBy(functions.col("sensor_id"),functions.window(timestamp,"1 hour","1 minute")).avg("W").sort("window").show(1000,false);
//        qwe.show(3000,false);
//
//        System.out.println("\nhere "+ qwe.count());

//		sch_3.select(functions.window(timestamp,"1 hour","1 minute")).printSchema();
//		Dataset<Row> result =sch_3.groupBy(functions.col("sensor_id"),functions.window(timestamp,"1 hour","1 minute")).avg("W").sort("window");
       // result.show(120,false);
//       result.withColumn("id", functions.row_number().over(Window.orderBy("window"))).where("id > 60 and id<121").show(12000,false);




	}
}
