package org.example.basics

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object DataFrameDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Session Application")
      .master("local")
      .getOrCreate()

    val intArray = Array(1,2,3,4,5,6,7,8)

    val rdd = spark.sparkContext.parallelize(intArray)
    val schema = StructType(
      StructField("Values", IntegerType, true) :: Nil
    )

    val rowRDD = rdd.map(element => Row(element))

    var df = spark.createDataFrame(rowRDD, schema)

    df.printSchema()
    df.show()


    /*df = spark.read.format("csv")
      .options(Map("header" -> "true")).load("src/main/resources/TechCrunchcontinentalUSA.csv");*/
    df = spark.read
        .option("sep", ",")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("src/main/resources/TechCrunchcontinentalUSA.csv")

    df.printSchema()
    df.show(10)


  }
}
