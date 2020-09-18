package com.example.etl

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Loader {
  def load(sparkSession: SparkSession, input: Map[String, DataFrame])
}
