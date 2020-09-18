package com.example.etl

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Transformer {

  def transform(sparkSession: SparkSession, input: Map[String, DataFrame]):Map[String, DataFrame]

}
