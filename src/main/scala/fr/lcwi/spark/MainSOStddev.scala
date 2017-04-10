package fr.lcwi.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions.stddev

/**
  * Created by chanelu on 10/02/2017.
  */
object MainSOStddev {

    case class Person(age: Int, birthdate: Int)

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
            .builder()
            .master("local")
            .appName("HbaseWriteTest")
            .getOrCreate()

        val personsDF = spark.createDataFrame(Seq(
            Person(24, 1980),
            Person(24, 1981),
            Person(24, 1990)
        ))

        val stddevCols: Array[Column] = personsDF.columns.map(c => stddev(c).as(c))

        val personsStddevDF: DataFrame = personsDF.agg(stddevCols.head, stddevCols.tail: _*)

        val columnsToKeep: Seq[String] = (personsStddevDF.first match {
            case r : Row => r.toSeq.toArray.map(_.asInstanceOf[Double])
        }).zip(personsStddevDF.columns)
            .filter(_._1 != 0) // your special condition is in the filter
            .map(_._2) // keep just the name of the column

        // select columns with stddev != 0
        val finalResult = personsDF.select(columnsToKeep.head, columnsToKeep.tail : _*)

        finalResult.show()
    }
}
