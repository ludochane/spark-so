package fr.lcwi.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, SparkSession}

/**
  * Created by chanelu on 10/02/2017.
  */
object MainSO2 {

    case class MyDateTime(ServerTime: String)

    case class CustomPair(x: Int, y: Int) {

        def canEqual(other: Any): Boolean = other.isInstanceOf[CustomPair]

        override def equals(other: Any): Boolean = other match {
            case that: CustomPair =>
                (that canEqual this) &&
                    ((x == that.x && y == that.y) || (x == that.y && y == that.x))
            case _ => false
        }

        override def hashCode(): Int = {
            31 * (x + y)
        }
    }

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
            .builder()
            .master("local")
            .appName("HbaseWriteTest")
            .getOrCreate()

        val dates = spark.createDataFrame(Seq(
            MyDateTime("1/20/2016 3:20:30 PM"),
            MyDateTime("1/20/2017 3:20:30 PM"),
            MyDateTime("1/20/2017 3:20:30 PM"),
            MyDateTime("1/20/2017 3:20:30 PM")
        ))

        val split_col: Column = split(dates("ServerTime"), " ")

        val result = dates
            .withColumn("Date", split_col.getItem(0))
            .withColumn("Time", split_col.getItem(1))

        result.show(false)

        val rdd = spark.sparkContext.parallelize(Seq(
            (12, 13),
            (13, 12)
        ))

        val customPairs: RDD[CustomPair] = rdd.map {
            case (x, y) => CustomPair(x, y)
        }

        import spark.implicits._

        customPairs.distinct().toDF.show()
    }
}
