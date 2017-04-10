package fr.lcwi.spark

/**
  * Created by chanelu on 23/03/2017.
  */
object MainXml {
    def main(args: Array[String]): Unit = {

        val strxml = <employees>
            | <employee><id>1</id><name>chris</name></employee>
            | <employee><id>2</id><name>adam</name></employee>
            | <employee><id>3</id><name>karl</name></employee>
            | </employees>

        val t = strxml.flatMap(line => line \\ "employee")

        t.map(l => (l \\ "id").text + "@" + (l \\ "name").text).foreach(println)
    }
}
