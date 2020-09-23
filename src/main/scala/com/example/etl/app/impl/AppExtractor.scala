package com.example.etl.app.impl

import com.example.etl.`trait`.Extractor
import org.apache.spark.sql.{DataFrame, SparkSession}

class AppExtractor extends Extractor {

  override def extract(sparkSession: SparkSession): Map[String, DataFrame] = {
    val emailDF = sparkSession
      .read
      .format("com.databricks.spark.avro")
      .load("/app/data/")
      .repartition(4)

    val dataFrameMap = Map.newBuilder[String, DataFrame]
    dataFrameMap .+= ("APP_DF" -> emailDF)

    dataFrameMap.result()
  }
}
