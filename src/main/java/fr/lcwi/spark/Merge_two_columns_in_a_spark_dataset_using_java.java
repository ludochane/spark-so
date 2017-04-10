package fr.lcwi.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * http://stackoverflow.com/questions/43280876/merge-two-columns-in-a-spark-dataset-using-java
 */
public class Merge_two_columns_in_a_spark_dataset_using_java {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("SO")
                .getOrCreate();

        Dataset<Row> df1 = spark.read().json("src/main/resources/json/Dataset.json");
        df1.printSchema();
        df1.show();

        Dataset<Row> df1Map = df1.select(functions.array("beginTime", "endTime"));
        df1Map.show();
    }
}
