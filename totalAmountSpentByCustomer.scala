package com.rakesh.spark

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
object totalAmountSpentByCustomer {
  
  def parseLine(line : String) = {
    
    val words = line.split(",")
    val customerId = words(0).toInt
    val amount= words(2).toFloat
    (customerId,amount)
  }
  
  def main(args : Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]","TotalAmountSpentByCustomer")
    
    val lines = sc.textFile("../customer-orders.csv")
    
    val parsedLines = lines.map(parseLine)
    
    //Now Rdd will have like customer Id and Amount pairs
    
    val keyValueRDD = parsedLines.reduceByKey((x,y)=> x+y)
    
    val results= keyValueRDD.collect()
    
    results.foreach(println)
    
    
    
  }
}