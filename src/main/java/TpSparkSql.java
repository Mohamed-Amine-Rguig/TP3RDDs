import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TpSparkSql {

    public static void main(String[] args) {
        // Initialize Spark session
        SparkSession spark = SparkSession.builder()
                .appName("TpSparkSql")
                .master("local[*]")
                .getOrCreate();

        // Load the CSV file into a DataFrame
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("incidents.csv");

        // create a temporary view
        df.createOrReplaceTempView("incidents");

        Dataset<Row> incidentsCountByService = spark.sql(
                "SELECT service, COUNT(*) as count FROM incidents GROUP BY service"
        );

        // 1) incidents count by service
        incidentsCountByService.show();

        // take only the year
        Dataset<Row> incidentCountByYear = spark.sql("SELECT SUBSTRING(date, 1, 4) as year, COUNT(*) as count FROM incidents GROUP BY year ORDER BY year DESC LIMIT 2");

        // 2) incidents count by year
        incidentCountByYear.show();
    }

}
