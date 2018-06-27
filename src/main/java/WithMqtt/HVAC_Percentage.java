package WithMqtt;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Date;

import static WithMqtt.PrevYearJoinWithCurrentSlot.getRowsByTableName;
import static WithMqtt.PrevYearJoinWithCurrentSlot.logsOff;

public class HVAC_Percentage {


    public static void main(String args[]) {
        String room = "\"power_k_erts_a\"";
        logsOff();
        SparkSession sparkSession = SparkSession.builder().appName("Java Spark App")
                .config("spark.sql.warehouse.dir", "~/spark-warehouse")
                .config("spark.executor.memory", "2g")
                .config("spark.driver.allowMultipleContexts", "true")

                .master("spark://10.129.149.14:7077")
                .getOrCreate();

        Dataset<Row> sch_3 = getRowsByTableName(sparkSession, "sch_3");


        Date now = new Date();


        long tm = System.currentTimeMillis();
        long epoch = now.getTime();
        System.out.println(epoch);
        epoch=epoch/1000 - 31536000;
//        epoch=epoch/1000;
        epoch=epoch-(epoch%60);
        long epoch2=epoch+86400;

        // 1495695343 1495695360
        //epoch=1495622280;
        //epoch2=1495622280+60;
        epoch=epoch;
        // epoch=1495693980;
        // epoch2=1495698000;
        System.out.println(epoch+" "+epoch2); // 12-1 pm
        sch_3 = sch_3.where("TS>="+epoch+" and TS<="+epoch2 + " and sensor_id="+room + " and W<12");

//        sch_3.show();

        System.out.println(sch_3.count());


        long tm1 = System.currentTimeMillis();

        System.out.println("TIME taken to fetch from db "+ (tm1-tm));
    }


}