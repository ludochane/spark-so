package fr.lcwi.spark

import java.sql.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.joda.time.DateTime

/**
  * Created by chanelu on 28/03/2017.
  */
object MainTimeSeries {

    case class Node(id: String, name: String, endTime: Date, value: Int)

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

        val df = spark.createDataFrame(Seq(
            Node("1", "node1", new Date(new DateTime(2017, 3, 24, 8, 30).getMillis), 5),
            Node("2", "node1", new Date(new DateTime(2017, 3, 24, 9, 0).getMillis), 3),
            Node("3", "node1", new Date(new DateTime(2017, 3, 24, 9, 30).getMillis), 8),
            Node("4", "node2", new Date(new DateTime(2017, 3, 24, 10, 0).getMillis), 5),
            Node("5", "node2", new Date(new DateTime(2017, 3, 24, 10, 30).getMillis), 3),
            Node("6", "node2", new Date(new DateTime(2017, 3, 24, 11, 0).getMillis), 1),
            Node("7", "node2", new Date(new DateTime(2017, 3, 24, 11, 30).getMillis), 3),
            Node("8", "node2", new Date(new DateTime(2017, 3, 24, 12, 0).getMillis), 5)
        ))

        val groupedDF = df
            .groupBy(
                col("name"),
                window(col("endTime"), "2 hour", "30 minute")
            )
            .agg(max("value").as("2_hour_max"))
            .filter(col("2_hour_max") < 6)
            .select("name")
            .distinct()

        groupedDF.show(false)
    }

}
