package fr.lcwi.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by chanelu on 27/03/2017.
  */
object MainCSV {

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

//        val fs: FileSystem = FileSystem.get(new Configuration())
//        val files: RemoteIterator[LocatedFileStatus] = fs.listFiles(new Path("src/main/resources/csv"), true)
//        val filePaths = new ListBuffer[String]
//        while (files.hasNext()) {
//            val file = files.next()
//            filePaths += file.getPath.toString
//        }

        val df: DataFrame = spark
            .read
            .options(Map(
                "delimiter" -> ";",
                "header" -> "true",
                "inferSchema" -> "true"
            ))
            .csv("src/main/resources/csv/**")

        df.show()

        //spark.createDataFrame(Seq(), classOf[Map[String, String]])

        df.explain(true)

    }

}
