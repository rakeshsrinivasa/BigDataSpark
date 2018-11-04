package com.rakesh.spark

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import scala.math.min

object FilterMinimumTempOfYear {
  
  def parseLine(line : String)= {
     val fields = line.split(",")
     val stationId = fields(0)
     val entryType = fields(2)
     val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
     (stationId,entryType,temperature)
  }
  
  
  def main(args : Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc=new SparkContext("local[*]","FilterMinTemp")
    
    val lines = sc.textFile("../1800.csv")
    
    val parsedLines = lines.map(parseLine)
    
    val minTemps = parsedLines.filter(x => x._2 == "TMIN")
    
    //Now Convert to Station Id, Temp TO key value rdd
    val stationTemparatures = minTemps.map(x => (x._1,x._3.toFloat))
    
    //ReduceBy Key the minimum of Temp
    
    val minTempFoundForStation = stationTemparatures.reduceByKey((x,y) => min(x,y))
    
    val results = minTempFoundForStation.collect()
    
    results.foreach(println)
    
  }
}