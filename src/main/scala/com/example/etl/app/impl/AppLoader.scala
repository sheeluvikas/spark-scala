package com.example.etl.app.impl

import com.example.app.SparkApp.logger
import com.example.etl.`trait`.Loader
import com.example.spark.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

class AppLoader extends Loader {

  override def load(sparkSession: SparkSession, input: Map[String, DataFrame], envMap: Map[String, String]): Unit = {

    val inputDF = input("APP_DF")

    SparkUtils.writeDFInParquet(sparkSession, inputDF, envMap("BUCKET_OUTPUT_PATH"))

    logger.info("Created the data in parquet at : " + envMap("BUCKET_OUTPUT_PATH"))
  }
}
