package org.example.basics

import java.beans.Transient
import java.io.InputStream

import org.apache.avro.Schema
import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.commons.lang3.StringUtils
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import util.control.Breaks._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

import scala.collection.mutable// remember to have spark core and spark sql have same version

/**
 * This class explains the usage of sparkSession and getting the data from
 * avro file, and creating a data frame, and then selecting one column as dataframe,
 * and using that dataframe to create another avro file
 */
object SparkFirst {

  @Transient lazy val logger: Logger = LogManager.getLogger("SparkSessionDemo")

  def findValue(df: DataFrame, path: String): Column = {
//    df(path).apply("context").as("Contexts")
//    df(path).apply("value").as("values"),
//    df.select(
//      when(col(path).isNotNull && ,
//      ))


      df(path).getField("context")
//      explode(df(path)).getField("context")
//        .apply("context").as("context")
//      explode(col(path)).apply("value").as("value")

//    df(path)
//  lit("hello")
//    .

  }

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("Spark Session Application")
      .master("local[*]")
      .getOrCreate()

    val schema: InputStream = getClass.getClassLoader.getResourceAsStream("userSchema.avsc")
    val avroSchema = new Schema.Parser().parse(schema)


    val avroDF = sparkSession.read.format("com.databricks.spark.avro")
      .option("avroSchema", avroSchema.toString())
      .load("src/main/resources/output.avro");

    //    avroDF.show()

    val path = "payload.alternativeIdentifiers"

//    avroDF.for
    val df = avroDF.select(
      col(path),
      findValue(col(path)).as("instrumentid")
    )

       df.printSchema()
//    df = df.withColumn("Alternatives", avroDF.col(path))

    import sparkSession.implicits._

    df.show(false)
  }

  val findValue = udf((arr :mutable.WrappedArray[Row]) =>{
//    println(arr[0])

    var ans:String = ""

    val prioritySeq = Array[String] ( "ISIN","TICKER")

    breakable {
      for (el <- prioritySeq) {
        arr.foreach(x => {
          if (StringUtils.equalsAnyIgnoreCase(x.getString(0), el)) {
            ans = x.getString(1)
            break
          }
        })
      }
    }
    ans
  })
}
