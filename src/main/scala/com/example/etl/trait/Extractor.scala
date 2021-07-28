package com.example.etl.`trait`

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Extractor {
  def extract(sparkSession: SparkSession, envMap: Map[String, String]):Map[String, DataFrame]
}
