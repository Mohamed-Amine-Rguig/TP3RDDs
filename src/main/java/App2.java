import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class App2 {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("Word Count").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);

        JavaRDD<String> rddLines=sc.textFile("sales.txt");
        JavaRDD<String[]> rddSales=rddLines.map(line -> {
            String city= line.split(",")[1];
            String amount = line.split(",")[3];

            return new String[]{city, amount};
        });
        JavaPairRDD<String,Double> salesPairRdd=rddSales.mapToPair(
                elem -> new Tuple2<>(elem[0], Double.parseDouble(elem[1]))
        );
        JavaPairRDD<String, Double> totalByCity = salesPairRdd.reduceByKey((a, b) -> a + b);
        totalByCity.foreach(pair -> System.out.println(pair._1 + " : " + pair._2));
    }
}
