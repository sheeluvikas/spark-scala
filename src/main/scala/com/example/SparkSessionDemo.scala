package com.example

/**
 * This class explains the usage of sparkSession and getting the data from
 * avro file, and creating a data frame, and then selecting one column as dataframe,
 * and using that dataframe to create another avro file
 */
import java.beans.Transient

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

object SparkSessionDemo {

  @Transient lazy val logger: Logger = LogManager.getLogger("SparkSessionDemo")

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("Spark Session Application")
      .master("local")
      .getOrCreate()

    val avroDF = sparkSession.read
      .format("com.databricks.spark.avro")
      .load("src/main/resources/userdata1.avro");

    avroDF.show()

    val emailDF = avroDF.select("kylosample.email")

    logger.info(s"The first two rows are : ${emailDF.show(2)}")
    print(emailDF.head)
    logger.info(emailDF.count)

    /** now create avro file from emailDF */
    emailDF.write.format("com.databricks.spark.avro").save("src/main/resources/Avro")
  }
}
