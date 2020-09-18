package com.example.etl.`trait`

import org.apache.spark.sql.{DataFrame, SparkSession}

trait ETL {

  def extract(sparkSession: SparkSession):Map[String, DataFrame]
  def transform(sparkSession: SparkSession, input: Map[String, DataFrame]):Map[String,DataFrame]
  def load(sparkSession: SparkSession, input: Map[String, DataFrame])

}
