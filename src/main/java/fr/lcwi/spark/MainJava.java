package fr.lcwi.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by chanelu on 22/03/2017.
 */
public class MainJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("SparkSessionFiles")
                .getOrCreate();


        List<String> persons = new ArrayList<>();
        persons.add("Marie");
        persons.add("Jean");
        persons.add("Denis");
        persons.add("Olivier");

        //JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> zipsRdd = spark.read().textFile("src/main/resources/zips").javaRDD();
        JavaRDD<String> peopleRDD = zipsRdd
                .mapPartitionsWithIndex((index, iter) -> {
                    if (index == 0 && iter.hasNext()) {
                        iter.next();
                        if (iter.hasNext()) {
                            iter.next();
                        }
                    }
                    return iter;
                }, true);

        peopleRDD.collect().forEach(System.out::println);
    }
}
