package fr.lcwi.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.xml.{Elem, NodeSeq, XML}

/**
  * Created by chanelu on 23/03/2017.
  */
object MainSparkXml {
    def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession
            .builder()
            .master("local")
            .appName("HbaseWriteTest")
            .getOrCreate()

        val filexml: RDD[(String, String)] = spark.sparkContext.wholeTextFiles("src/main/resources/xml/test1.xml")

        val lines: RDD[Elem] = filexml.map(line => XML.loadString(line._2))

        val ft: RDD[NodeSeq] = lines.map(l => l \\ "employee")

        ft.map(l => (l \\ "id").text + "@" + (l \\ "name").text).foreach(println)
    }
}
