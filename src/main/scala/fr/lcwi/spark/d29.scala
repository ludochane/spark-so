package fr.lcwi.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by chanelu on 29/03/2017.
  */
object d29 {
    def main(args: Array[String]) {
        val name: String = "myspark"
        val master: String = "local[1]"
        val conf: SparkConf = new SparkConf().setAppName(name).setMaster(master)
        val spContext: SparkContext = new SparkContext(conf)
        val file: RDD[String] = spContext.textFile("zed/text.csv")
        val mapped = file.map(s => s.length)
    }
}
