package com.example.etl.`trait`

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Transformer {

  def transform(sparkSession: SparkSession, input: Map[String, DataFrame], envMap: Map[String, String]):Map[String, DataFrame]

}
