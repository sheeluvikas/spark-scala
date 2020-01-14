package org.example.basics

import org.apache.spark.{SparkConf, SparkContext}

object SparkFirst { // object are kind of static class

  def main(args: Array[String]): Unit = {
    /** Set the spark conf */
    val sparkConf = new SparkConf()
    sparkConf.setAppName("firstSparkApplicaiton")
    sparkConf.setMaster("local")

    /** now create the spark context */
    val sc = new SparkContext(sparkConf)

    val intArray = Array(1,2,3,4,5,6,7,8,9,10)
    val arrayRDD = sc.parallelize(intArray, 5) /** Setting the number of partitions */

    arrayRDD.collect().foreach(println)
    println("the size of the partitions : "+ arrayRDD.partitions.size)

    val fileRDD = sc.textFile("src/main/resources/test.txt")
    println("******************** fileRDD *******************")
    fileRDD.collect().foreach(println)

  }

}