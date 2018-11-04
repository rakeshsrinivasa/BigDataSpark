package com.rakesh.spark

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object WordCountUsingFlatMap {
  
  
  def main(args : Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]","WordCounterUsingFlatMap")
    
    val lines = sc.textFile("../book.txt")
    
    val words = lines.flatMap(x => x.split(" "))
    
    
   val wordcount= words.countByValue()
    
    wordcount.foreach(println)
    
   /* val feedbackCounter = words.filter(x => x == "feedback")
    
    val feedbackWordCount = feedbackCounter.countByValue()
    feedbackWordCount.foreach(println)*/
    
  }
}