package com.example.app

import java.beans.Transient

import com.example.etl.app.impl.EtlFactory
import com.example.spark.utils.SparkUtils
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.DataFrame

/**
 * This application works in the spark-submit mode, reads and stores the file in hdfs locations
 * the script file which runs this application is : bin/spark_app.sh
 *
 */
object SparkApp {
  @Transient lazy val logger: Logger = LogManager.getLogger("SparkApp")

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkUtils.buildSparkSessionApp("MySparkApp", null)
    logger.info("Created the sparkSession !!!")

    val etl = EtlFactory()

    var envMap = Map.newBuilder[String, String].result()

    logger.info("*******************")
    logger.info(System.getProperty("BUCKET_INPUT_PATH"))
    logger.info(System.getProperty("BUCKET_OUTPUT_PATH"))
    logger.info("*******************")

    envMap
      .+=(
        "BUCKET_INPUT_PATH" ->System.getProperty("BUCKET_INPUT_PATH"), // "/app/data/"
        "BUCKET_OUTPUT_PATH" -> System.getProperty("BUCKET_OUTPUT_PATH")    // "/app/hive/file"
      )


    etl match {
      case Some(i) =>
        val extract : Map[String, DataFrame] = i.extract(sparkSession, envMap)
        val transform : Map[String, DataFrame] = i.transform(sparkSession, extract, envMap)
        i.load(sparkSession, transform, envMap)
      case None =>
        System.exit(1)
    }
  }
}
