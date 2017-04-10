package fr.lcwi.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by chanelu on 30/03/2017.
  */
object MainRecursive {

    case class Data(code: Long, value: String)

    def main(args: Array[String]): Unit = {
        val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()

        val data = Seq(
            Data(1,"{NUM.0002}*{NUM.0003}"),
            Data(2,"{NUM.0004}+{NUM.0003}"),
            Data(3,"END(6)"),
            Data(4,"END(4)"),
            Data(5,"{NUM.0002}")
        )

        val initialDF = sparkSession.createDataFrame(data)
        val endDF = initialDF.filter(!(col("value") contains "{NUM"))
        val numDF = initialDF.filter(col("value") contains "{NUM")

        val resultDF = endDF.union(replaceNumByEnd(initialDF, numDF))
        resultDF.show(false)
    }


    val parseNumUdf = udf((value: String) => {
        if (value.contains("{NUM")) {
            val regex = """.*?\{NUM\.(\d+)\}.*""".r
            value match {
                case regex(code) => code.toLong
            }
        } else {
            -1L
        }
    })

    val replaceUdf = udf((value: String, replacement: String) => {
        val regex = """\{NUM\.(\d+)\}""".r
        regex.replaceFirstIn(value, replacement)
    })

    def replaceNumByEnd(initialDF: DataFrame, currentDF: DataFrame): DataFrame = {
        if (currentDF.count() == 0) {
            currentDF
        } else {
            val numDFWithCode = currentDF
                .withColumn("num_code", parseNumUdf(col("value")))
                .withColumnRenamed("code", "code_original")
                .withColumnRenamed("value", "value_original")

            val joinedDF = numDFWithCode.join(initialDF, numDFWithCode("num_code") === initialDF("code"))

            val replacedDF = joinedDF.withColumn("value_replaced", replaceUdf(col("value_original"), col("value")))

            val nextDF = replacedDF.select(col("code_original").as("code"), col("value_replaced").as("value"))

            val endDF = nextDF.filter(!(col("value") contains "{NUM"))
            val numDF = nextDF.filter(col("value") contains "{NUM")

            endDF.union(replaceNumByEnd(initialDF, numDF))
        }
    }

}
