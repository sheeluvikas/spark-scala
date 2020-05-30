package com.example

import com.example.spark.utils.SparkUtils
import org.apache.spark.sql.functions.col

object SparkApp {


  def main(args: Array[String]): Unit = {
    val sparkSession = SparkUtils.buildSparkContextApp("MySparkApp", null)

    // read the avro file

    val emailDF = sparkSession
      .read
      .format("com.databricks.spark.avro")
      .load("src/main/resources/userData1.avro")

    val outDF = emailDF.select(
      col("id"),
      col("first_name"),
      col("last_name"),
      col("email"),
      col("gender"),
      col("country")
    )

    SparkUtils.writeDFInParquet(sparkSession, outDF, "hdfs:///app/hive/file")

  }
}
