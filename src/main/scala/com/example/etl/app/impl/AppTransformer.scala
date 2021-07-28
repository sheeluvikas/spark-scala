package com.example.etl.app.impl

import com.example.etl.`trait`.Transformer
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class AppTransformer extends Transformer {

  override def transform(sparkSession: SparkSession, input: Map[String, DataFrame], envMap: Map[String, String]): Map[String, DataFrame] = {
    val inputDF = input("APP_DF")

    val outDF = inputDF.select(
      col("id"),
      col("first_name"),
      col("last_name"),
      col("email"),
      col("gender"),
      col("country")
    )


    val dataFrameMap = Map.newBuilder[String, DataFrame]
    dataFrameMap .+= ("APP_DF" -> outDF)

    dataFrameMap.result()
  }
}
