package org.example.basics

import java.beans.Transient

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession // remember to have spark core and spark sql have same version

/**
 * This class explains the usage of sparkSession and getting the data from
 * avro file, and creating a data frame, and then selecting one column as dataframe,
 * and using that dataframe to create another avro file
 */
object SparkFirst {

  @Transient lazy val logger: Logger = LogManager.getLogger("SparkSessionDemo")

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("Spark Session Application")
      .master("local")
      .getOrCreate()

    val avroDF = sparkSession.read
      .format("com.databricks.spark.avro")
      .load("src/main/resources/output.avro");

    avroDF.show()

    val outputDF = avroDF.select("payload.identifier.id")

    outputDF.show()

  }
}
