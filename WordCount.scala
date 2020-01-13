package com.solix.spark.SparkScala

import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.log4j._
import scala.math._

object WordCount {
  def main(args: Array[String]) {
    val sc = new SparkContext("local[*]", "WordCount")
    sc.setLogLevel("ERROR")

    val lines = sc.textFile("C:\\work\\SparkScala\\book.txt")
    val ignoreWordsList = List("is", "a", "are", "the")

    val words = lines.flatMap(x => x.split("\\W+"))
    val lowerWords = words.map(x => x.toLowerCase())

    val filterWords = lowerWords.filter(!ignoreWordsList.contains(_))

    val mapWithOne = filterWords.map(x => (x, 1))
    val countWords = mapWithOne.reduceByKey((x, y) => x + y)
    val countSorted = countWords.map(x => (x._2, x._1)).sortByKey()

    for (result <- countSorted) {
      val count = result._1
      val word = result._2

      println(s"$word:$count")
    }
    //count.toSeq.sorted.foreach(println)
  }
}