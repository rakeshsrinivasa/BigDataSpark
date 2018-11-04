package com.rakesh.spark

import org.apache.spark.SparkContext

object BasicExample {
  
  
  def main(args : Array[String]) = {
    
    val sc= new SparkContext("local[*]", "BasicTransformation")
    
    val rdd = sc.parallelize(List(1,2,3,4))
    
    val squareMe = rdd.map(doubleMe)
    
    println(" The OutPut " +squareMe.toString())
    
    println("Collect results " +squareMe.collect())    
    
    squareMe.take(5).foreach(println)
  } 
  
  def doubleMe(x : Int) : Int = {
   return  x*x
  }
  
}
