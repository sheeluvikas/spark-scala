package com.example.spark.utils

import org.apache.spark.sql.SparkSession

object SparkUtils {

  def buildSparkContext(appName: String): SparkSession = {
    val sparkSession = SparkSession.builder()
      .appName("Spark Session Application")
      .master("local[*]")
      .getOrCreate()
    sparkSession
  }
}
