package fr.lcwi.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by chanelu on 10/02/2017.
  */
object MainSO3 {

    case class Basic(name: String, age: Int)

    def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession
            .builder()
            .master("local")
            .appName("HbaseWriteTest")
            .getOrCreate()

        val df = spark.createDataFrame(Seq(
            Basic("sfdaaa,aa", 12),
            Basic("   bb   ", 22),
            Basic("   bb   ", 22),
            Basic("   bb   ", 22)
        ))

        val rdd: RDD[Row] = df.rdd.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(2) else iter }

        rdd.collect().foreach(println)

//        df.write
//            .mode(SaveMode.Overwrite)
//            .format("csv")
//            .option("header", "true")
////            .option("delimiter", ";")
//            .option("quote", "\"")
//            .option("quoteMode", "ALL")
//            .save("essai.csv")
    }
}
