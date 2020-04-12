package org.example.basics

import java.io.InputStream

import org.apache.avro.Schema
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, lit, when}


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

    /************************** Read from Avro schema *******************************/
    print("*************** AVRO **************")

    val schemaStream: InputStream = getClass.getClassLoader.getResourceAsStream("customSchema.avsc")
    val avroSchema = new Schema.Parser().parse(schemaStream);
        val avroDF = spark.read
      .format("com.databricks.spark.avro")
      .option("avroSchema", avroSchema.toString)
      .load("src/main/resources/userdata1.avro");

    avroDF.printSchema()
    avroDF.show(50)

    val countryNameDF = avroDF.select(
      col("first_name").cast(StringType).as("First_Name"),
      col("last_name").cast(StringType).as("Last_Name"),
      col("email").as("Email_Id")
    )
    print(countryNameDF.show())

  }
}
