package com.example

import java.beans.Transient

import com.example.spark.utils.SparkUtils
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.functions.col

object SparkApp {
  @Transient lazy val logger: Logger = LogManager.getLogger("SparkApp")

  def main(args: Array[String]): Unit = {

    logger.info("********** Entered the main method !!!")
    val sparkSession = SparkUtils.buildSparkContextApp("MySparkApp", null)

    // read the avro file

    logger.info("********** Created the sparkSession !!!")
    val emailDF = sparkSession
      .read
      .format("com.databricks.spark.avro")
      .load("/app/data/userdata1.avro")

    logger.info("******** Created the emailDF ****")
    val outDF = emailDF.select(
      col("id"),
      col("first_name"),
      col("last_name"),
      col("email"),
      col("gender"),
      col("country")
    )

    logger.info("************* outDF Created !!!")

    SparkUtils.writeDFInParquet(sparkSession, outDF, "/app/hive/file")

    logger.info("******** Created the data in parquet at : /app/hive/file")

  }
}
