package com.example.etl
import com.example.app.SparkApp.logger
import com.example.spark.utils.SparkUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

class AppLoader extends Loader {

  override def load(sparkSession: SparkSession, input: Map[String, DataFrame]): Unit = {

    val inputDF = input("APP_DF")

    logger.info("************* outDF Created !!!")

    SparkUtils.writeDFInParquet(sparkSession, inputDF, "/app/hive/file")

    logger.info("******** Created the data in parquet at : /app/hive/file")
  }
}
