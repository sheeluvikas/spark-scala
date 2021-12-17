package com.example.others

import org.apache.avro.Schema
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import java.io.InputStream

object DataFrameDemo2 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Session Application")
      .master("local")
      .getOrCreate()


    val employeeDF = spark.read
      .option("sep", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/Employee.csv")

    var managerDF = spark.read
      .option("sep", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/Manager.csv")

    employeeDF.show(false)
    managerDF.show(false)

    managerDF = managerDF.withColumn("manager_salary", col("salary"))

    val outDF = employeeDF.where(col("salary") > 30000).join(managerDF, col("manager_id") === managerDF("id"),
      "inner")
      .where(employeeDF("salary") > managerDF("salary"))

    outDF.show(false)

    //************************************************************************************


  }
}
