package com.example.etl.`trait`

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Loader {
  def load(sparkSession: SparkSession, input: Map[String, DataFrame], envMap: Map[String, String])
}
