package fr.lcwi.spark

import org.apache.spark.ml.param.{Param, ParamMap, ParamPair}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by chanelu on 10/02/2017.
  */
object MainSO {

    case class Basic(name: String, age: Int, other: Int)

    def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession
            .builder()
            .master("local")
            .appName("HbaseWriteTest")
            .getOrCreate()

        val df: DataFrame = spark.createDataFrame(Seq(
            Basic("Jean", 20, 1),
            Basic("Jean", 20, 1),
            Basic("Marie", 20, 1),
            Basic("Ludo", 20, 1),
            Basic("Ludo", 20, 2),
            Basic("Ludo", 20, 3),
            Basic("Ludo", 23, 1),
            Basic("Ludo", 23, 2),
            Basic("Marie", 23, 2)
        ))

//        val count = df.select("name", "age")
//            .distinct()
//            .groupBy("name", "age")
//            .count()
//            .filter(col("count") > 1)
//            .select("name", "age")
//            .count()
//
//        println(count)

        df
            .select("name", "age", "other")
            .distinct()
            .filter(col("age") <=> 20)
            .groupBy("name", "age")
            .count()
            .filter(col("count") > 1)
            .select("name")
            .show()

        df
            .select("id", "aid", "DId", "dd", "mm", "yy", "TO")
            .distinct()
            .filter(col("cid") <=> 1)
            .groupBy("id", "aid", "DId", "dd", "mm", "yy")
            .count()
            .filter(col("count") > 1)
            .select("id", "aid", "DId", "dd", "mm", "yy")

        val params: ParamMap = ParamMap(ParamPair(new Param("parent", "name", "doc"), 1))

        params.toSeq.foreach(pair => {
                println(pair.param)
                println(pair.value)
            }
        )

        params(new Param("parent", "name", "doc"))

    }
}
