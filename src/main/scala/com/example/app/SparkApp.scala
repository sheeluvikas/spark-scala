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

    val sparkSession = SparkUtils.buildSparkContextApp("MySparkApp", null)
    logger.info("Created the sparkSession !!!")

    val etl = EtlFactory()

    etl match {
      case Some(i) =>
        val extract : Map[String, DataFrame] = i.extract(sparkSession)
        val transform : Map[String, DataFrame] = i.transform(sparkSession, extract)
        i.load(sparkSession, transform)
      case None =>
        System.exit(1)
    }
  }
}
