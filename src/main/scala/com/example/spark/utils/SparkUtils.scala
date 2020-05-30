package com.example.spark.utils

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkUtils {

  def buildSparkContext(appName: String, envMap: Map[String, String]): SparkSession = {
    val sparkSession = SparkSession.builder()
      .appName("Spark Session Application")
      .master("local[*]")
      .getOrCreate()
    sparkSession
  }

  /**
   * This method returns a SparkSession with the given appName and environmentMap
   * The environment Map contains the variables like executor.memory, executor.instances, executor.cores,
   * driver.memory, compression codec format etc.
   *
   * @param appName
   * @param envMap
   * @return
   */
  def buildSparkContextApp(appName: String, envMap: Map[String, String]): SparkSession ={

    val sparkSession = SparkSession.builder()
      .appName(appName)
//      .enableHiveSupport() // TODO : Learn about it
      // TODO : Put here the config properties like spark.executor.memory, cores, driver.memory etc
//      .config("spark.executor.memory", envMap("executor.memory"))
      .getOrCreate()

    sparkSession

  }

  def writeDFInParquet(sparkSession: SparkSession, dataFrame: DataFrame, path: String): Unit ={
    dataFrame.write
      .mode(SaveMode.Overwrite)
      .save(path)
  }
}