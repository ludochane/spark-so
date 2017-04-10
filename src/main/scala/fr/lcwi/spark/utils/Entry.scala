package fr.lcwi.spark.utils

/**
  * Created by chanelu on 10/02/2017.
  */
case class Entry(id: String, nom_table: String, pk: String, value: String, nb: Int)

case class EntryMap(id: String, nom_table: String, pk: Map[String, String], nb: Int)
