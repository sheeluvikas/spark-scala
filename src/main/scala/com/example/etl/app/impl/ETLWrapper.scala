package com.example.etl.app.impl

import com.example.etl.`trait`.{ETL, Extractor, Loader, Transformer}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ETLWrapper(val extractor: Extractor,
                 val transformer: Transformer,
                 val loader: Loader) extends ETL {
  override def extract(sparkSession: SparkSession, envMap: Map[String, String]): Map[String, DataFrame] = extractor.extract(sparkSession, envMap)

  override def transform(sparkSession: SparkSession, input: Map[String, DataFrame], envMap: Map[String, String]): Map[String, DataFrame] = transformer.transform(sparkSession, input, envMap)

  override def load(sparkSession: SparkSession, input: Map[String, DataFrame], envMap: Map[String, String]): Unit = loader.load(sparkSession, input, envMap)
}
