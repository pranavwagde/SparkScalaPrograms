package com.solix.spark.SparkScala

//Import Statements for the Spark and SpatkContext
import org.apache.spark.SparkContext._
import org.apache.spark._
import org.apache.log4j._
import scala.math._

object WordCount {
  //Main function started
  def main(args: Array[String]) {
    //Initialization of SparkContext
    val sc = new SparkContext("local[*]", "WordCount")
    
    //Setting Log level to ERROR, to avoid extra messages in output
    sc.setLogLevel("ERROR")
    
    //Reading the book.txt file and store it in variable `lines`
    val lines = sc.textFile("C:\\work\\SparkScala\\book.txt")
    
    //List of words, which can be ignore while word count
    val ignoreWordsList = List("is", "a", "are", "the")
    
    //Converting the lines to words, separated by Space
    val words = lines.flatMap(x => x.split("\\W+"))
    
    //Connverting all the words to Lowecase, to ignore counting the same word in different fashion based on CASE
    val lowerWords = words.map(x => x.toLowerCase())
    
    //Filter the words, if present in IngoreWord List
    val filterWords = lowerWords.filter(!ignoreWordsList.contains(_))
     
    //Map all the words to (word,1)
    val mapWithOne = filterWords.map(x => (x, 1))
    
    //Count the words by Key
    val countWords = mapWithOne.reduceByKey((x, y) => x + y)
    
    //Sort the Words by letter
    val countSorted = countWords.map(x => (x._2, x._1)).sortByKey()
    
    //Print the list of words with count
    for (result <- countSorted) {
      val count = result._1
      val word = result._2

      println(s"$word:$count")
    }
    //count.toSeq.sorted.foreach(println)
  }
}
