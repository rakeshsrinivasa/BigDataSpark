package com.rakesh.spark

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

object WordCounterWithSortAndUsingKeyValueRDD {
  
  def main(args : Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc =new SparkContext("local[*]", "WordCounterUsingSortAndKeyValueRDD")

    val lines= sc.textFile("../book.txt")
    
    val words = lines.flatMap(x=> x.split("\\W+"))
    
    val lowercaseRDD= words.map(x=> x.toLowerCase())
    
    //To sort by key , having key value rdd will be useful and efficient to distrubute in cluster
    val keyValueRDD = lowercaseRDD.map(x=>(x,1)).reduceByKey((x,y)=>x+y)
    
    
    //Now change the RDD tuple from (word,count) to (count,word) to sort using Key which will be easier and faster
    val sortedRDD = keyValueRDD.map(x=>(x._2,x._1)).sortByKey()
    
    println("Word COunter ")
    for( res <- sortedRDD) {
      val count = res._1
      val word = res._2
      println(s"$word : $count")
    }
    
        
  }
}