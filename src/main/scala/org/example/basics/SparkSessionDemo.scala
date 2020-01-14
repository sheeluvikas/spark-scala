package org.example.basics

import org.apache.spark.sql.SparkSession // remember to have spark core and spark sql have same version

object SparkSessionDemo {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("Spark Session Application")
      .master("local")
      .getOrCreate()

    val csvRDD = sparkSession.sparkContext.textFile("src/main/resources/TechCrunchcontinentalUSA.csv")

    println("Total rows are :" + csvRDD.count())

    val withoutHeaderRDD = csvRDD.filter(line => !line.contains("permalink,company,numEmps,category,city,state,fundedDate,raisedAmt,raisedCurrency,round"))
    println("*********************")
    withoutHeaderRDD.foreach(println)

  }

}
