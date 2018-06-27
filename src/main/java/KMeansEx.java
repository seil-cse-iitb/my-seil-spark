import handler.ConfigHandler;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;

import java.util.Arrays;
import java.lang.*;
import java.util.Iterator;

public class KMeansEx {



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
    public static void main(String args[]) {


        String room="\"power_k_erts_a\"";
        logsOff();
        SparkSession sparkSession = SparkSession.builder().appName("Java KMeans")
                .config("spark.sql.warehouse.dir", "~/spark-warehouse")
                .config("spark.executor.memory", "2g")
                .config("spark.driver.allowMultipleContexts", "true")
                .master("local[4]")
                .getOrCreate();

        Dataset<Row> sch_3 = getRowsByTableName(sparkSession, "sch_3");
      //  long epoch=
       // sch_3 = sch_3.where("TS>="+epoch+" and TS<="+epoch2 + " and sensor_id="+room);
        JavaRDD<Row> sch_3_RDD=sch_3.javaRDD();

        String path = "/home/awisha/Downloads/spark-2.2.1-bin-hadoop2.7/data/mllib/kmeans_data.txt";

        SparkConf conf = new SparkConf().setAppName("KMeansEx").setMaster("local[2]");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<Row> data = sch_3_RDD;


        JavaRDD<Vector> parsedData = sch_3_RDD
                .map(new Function<Row, Vector>() {
                    public Vector call(Row row) throws Exception {
                        return (Vector)row.get(0);
                    }
                });


       /* JavaRDD<Vector> parsedData = data.map(s -> {
            String[] sarray = s.split(" ");
            double[] values = new double[sarray.length];
            for (int i = 0; i < sarray.length; i++) {
                values[i] = Double.parseDouble(sarray[i]);
            }
            return Vectors.dense(values);
        });

        parsedData.cache();*/

        // Cluster the data into two classes using KMeansEx
        int numClusters = 2;
        int numIterations = 2;
        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);


        System.out.println("Cluster centers:");

        for (Vector center : clusters.clusterCenters()) {
            System.out.println(" " + center);
        }
        double cost = clusters.computeCost(parsedData.rdd());

        System.out.println("Cost: " + cost);

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(parsedData.rdd());

        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

// Save and load model

        clusters.save(jsc.sc(), "target/org/apache/spark/JavaKMeansExample/KMeansModel");
        KMeansModel sameModel = KMeansModel.load(jsc.sc(),
                "target/org/apache/spark/JavaKMeansExample/KMeansModel");

    }


}
