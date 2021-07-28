package com.example.etl.app.impl

import com.example.etl.`trait`.Extractor
import org.apache.spark.sql.{DataFrame, SparkSession}

class AppExtractor extends Extractor {

  override def extract(sparkSession: SparkSession, envMap: Map[String, String]): Map[String, DataFrame] = {
    val emailDF = sparkSession
      .read
      .format("com.databricks.spark.avro")
      .load(envMap("BUCKET_INPUT_PATH"))
      .repartition(4)

//    "/app/data/"

    val dataFrameMap = Map.newBuilder[String, DataFrame]
    dataFrameMap .+= ("APP_DF" -> emailDF)

    dataFrameMap.result()
  }
}
