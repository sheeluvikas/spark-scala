package com.example.etl

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Extractor {
  def extract(sparkSession: SparkSession):Map[String, DataFrame]
}
