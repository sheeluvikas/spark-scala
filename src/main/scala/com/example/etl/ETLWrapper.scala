package com.example.etl
import org.apache.spark.sql.{DataFrame, SparkSession}

class ETLWrapper(val extractor: Extractor,
                 val transformer: Transformer,
                 val loader: Loader) extends ETL {
  override def extract(sparkSession: SparkSession): Map[String, DataFrame] = extractor.extract(sparkSession)

  override def transform(sparkSession: SparkSession, input: Map[String, DataFrame]): Map[String, DataFrame] = transformer.transform(sparkSession, input)

  override def load(sparkSession: SparkSession, input: Map[String, DataFrame]): Unit = loader.load(sparkSession, input)
}
