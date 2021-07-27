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
   * For spark-history-server :
   * go to the SPARK_HOME directory : /usr/local/Cellar/apache-spark/3.1.2/libexec/sbin
   * and run the spark-history-server.sh
   * but remember to create the directory : mkdir /tmp/spark-events
   * and set the configurations done below :
   * spark.eventLog.enabled
   * spark.eventLog.dir
   * spark.history.fs.logDirectory
   *
   * once the application is running, go to http://localhost:18080 to view the spark-history details
   *
   * Check here for more details : https://luminousmen.com/post/spark-history-server-and-monitoring-jobs-performance
   *
   * @param appName
   * @param envMap
   * @return
   */
  def buildSparkSessionApp(appName: String, envMap: Map[String, String]): SparkSession ={

    val sparkSession = SparkSession.builder()
      .appName(appName)
      .enableHiveSupport() // TODO : Learn about it
      // TODO : Put here the config properties like spark.executor.memory, cores, driver.memory etc
//      .config("spark.executor.memory", envMap("executor.memory"))
      .config("spark.eventLog.enabled", true)
      .config("spark.eventLog.dir", "file:/tmp/spark-events")
      .config("spark.history.fs.logDirectory", "file:/tmp/spark-events")
      .getOrCreate()

    sparkSession

  }

  def writeDFInParquet(sparkSession: SparkSession, dataFrame: DataFrame, path: String): Unit ={

    dataFrame.write
      .mode(SaveMode.Overwrite)
      .option("path", path)
      .saveAsTable("default.student")

    /** in order to saveAsTable work, the application must be running in cluster mode, and not client mode
     * Otherwise, the table will be created in the local by default derby database, and will not be visible
     * in hive.
     *
     * refer this article for more info on metastore :
     *  https://dzone.com/articles/hive-metastore-a-basic-introduction
     *
     *  for errors :
     *  https://stackoverflow.com/questions/47523575/messagehive-schema-version-1-2-0-does-not-match-metastores-schema-version-2-1/47624770
     *
     * */
  }
}