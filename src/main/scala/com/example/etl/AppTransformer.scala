package com.example.etl
import com.example.app.SparkApp.logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class AppTransformer extends Transformer {

  override def transform(sparkSession: SparkSession, input: Map[String, DataFrame]): Map[String, DataFrame] = {
    val inputDF = input("APP_DF")

    logger.info("******** Created the emailDF ****")
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
