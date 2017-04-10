package fr.lcwi.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by chanelu on 27/03/2017.
  */
object MainJoinIntFloat {

    case class Person(name: String, age: Int)
    case class Person2(name: String, age: Float)

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

        val df1: DataFrame = spark.createDataFrame(Seq(Person("Jean", 20)))
        val df2: DataFrame = spark.createDataFrame(Seq(Person2("Jean2", 20.0F)))

        val df: DataFrame = df1.join(df2, Seq("age"), "right_outer").drop(df1("age"))

        df.show()
    }

}
