package com.rakesh.spark

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext

//Count the average number of friends by age
object AverageFriendsByAge {
  
  def parseLine(line : String) ={
    
    val fields=line.split(",")
    
    val columnAge=fields(2).toInt
    
    val noOfFriends = fields(3).toInt
    
    (columnAge,noOfFriends)  //Return a tuple of age, no of friends
  }
  
 
  
  def main(args : Array[String])={
    
    //Set the Logger Level
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val sc = new SparkContext("local[*]","FriendsByAge")
    
    val linesRDD = sc.textFile("../fakefriends.csv")
    
    val parsedRDD = linesRDD.map(parseLine)
    
    //Now Parsed RDD will contain rows of (Age, No of Friends) like (23,4) (23,45) , (45, 11)
    
    //Now Convert the RDD into Key Value RDD
    
    val keyValRDD = parsedRDD.mapValues(x =>(x,1)).reduceByKey( (x,y) => (x._1 + y._1,x._2 + y._2) )
    
    //The above converts the indiviual rdd into key Value rdd like 
    // 23, (4,1) 
    // 23 , (45,1)
    
    // Reduce By Key works like below
    // 23, (49,2) where 49 is number of total friends for age 23 aqnd number of age 23 people is 2
    
   val averageByAge = keyValRDD.mapValues(x=> x._1/x._2).sortByKey()
   
   //The above calculates the average
   //23 , (49/2) == 23, 24
   
   //Therefore average number of friends for age group 23 = 24
   
   //val results=averageByAge.collect().toSeq.sortBy(_._1)
      val results=averageByAge.collect()
        results.foreach(println)
   
   
  }
}