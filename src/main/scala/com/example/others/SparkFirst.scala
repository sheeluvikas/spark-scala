package com.example.others

import java.beans.Transient
import java.io.InputStream

import com.example.spark.utils.SparkUtils
import com.example.spark.utils.UserDefinedFunctions.{findValue, getElementConditional}
import org.apache.avro.Schema
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.functions.{col, lit}

/**
 * This class explains the usage of sparkSession and getting the data from
 * avro file, and creating a data frame, and then selecting one column as dataframe,
 * and using that dataframe to create another avro file
 */
object SparkFirst {

  @Transient lazy val logger: Logger = LogManager.getLogger("SparkSessionDemo")

  def main(args: Array[String]): Unit = {

    logger.info("********************")
    System.out.println("*************************")
    val sparkSession = SparkUtils.buildSparkSessionApp("SparkFirst", null)

    val schema: InputStream = getClass.getClassLoader.getResourceAsStream("userSchema.avsc")
    val avroSchema = new Schema.Parser().parse(schema)

    val avroDF = sparkSession.read.format("com.databricks.spark.avro")
      .option("avroSchema", avroSchema.toString())
      .load("src/main/resources/output.avro");

    val path = "payload.alternativeIdentifiers"

    val df = avroDF.select(
      col(path),
      findValue(col(path)).as("instrumentid"),
      getElementConditional(col("payload.classifications"),
        lit("schemeName"),
        lit("Classification of financial instruments"),
        lit("description")).as("prdsname"),
      getElementConditional(col("payload.partyRoles"), lit("role"), lit("ISSUER"), lit("partyDetail.primaryName"))
    )

    df.printSchema()

    df.show(false)
  }


}
