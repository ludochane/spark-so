package fr.lcwi.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
  * Created by chanelu on 10/02/2017.
  */
object MainSOStruct {

    case class Root(subtable: Subtable)
    case class Subtable(col1: Seq[Int], col2: String)

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
            .builder()
            .master("local")
            .appName("HbaseWriteTest")
            .getOrCreate()


//        val df = spark.createDataFrame(Seq(
//            Root(Subtable(Seq(1, 2, 3), "toto")),
//            Root(Subtable(Seq(10, 20, 30), "tata"))
//        ))
//
//        val myUdf = udf((col1: Seq[Int]) => col1.map(_ + 2))
//
//        val result = df.withColumn("col1_new", myUdf(df("subtable.col1")))
//
//        result.printSchema()
//
//        result.show(false)
//
//
//
//
//        val myUdf2 = udf((subtable: Row) =>
//            Subtable(subtable.getSeq[Int](0).map(_ + 2), subtable.getString(1))
//        )
//        val result2 = df.withColumn("subtable_new", myUdf2(df("subtable")))
//        result2.printSchema()
//        result2.show(false)

        val serverLog = spark.sparkContext.textFile("build.sbt")
        val jsonRows: RDD[Subtable] = serverLog.mapPartitions(partition => {
            //val txfm = new JsonParser //*jar to parse logs to json*//
            partition.map(line => {
                Subtable(Seq(1), line)
            })
        })

        import spark.implicits._

        jsonRows.toDF().printSchema()

        jsonRows.take(2).foreach(println)

    }
}
