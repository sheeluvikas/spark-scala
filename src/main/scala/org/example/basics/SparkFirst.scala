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
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericRowWithSchema}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

import scala.collection.mutable// remember to have spark core and spark sql have same version

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
      .master("local[*]")
      .getOrCreate()

    val schema: InputStream = getClass.getClassLoader.getResourceAsStream("userSchema.avsc")
    val avroSchema = new Schema.Parser().parse(schema)


    val avroDF = sparkSession.read.format("com.databricks.spark.avro")
      .option("avroSchema", avroSchema.toString())
      .load("src/main/resources/output.avro");

    //    avroDF.show()

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

  def getSubsequentElements(returningPath:String, elements:Row):String = {
    val paths = returningPath.split("\\.")
    var value:String = ""
    var element = elements
    for(path <- paths){
      if(element.getAs(path).isInstanceOf[GenericRowWithSchema]){
        element = element.getAs(path)
      }
      else {
        value = element.getAs(path)
      }
    }
    value
  }

  def getElementConditional = udf((classfications: mutable.WrappedArray[Row], matchingElement:String, matchingValue:String, returningValue:String) => {
    var value:String = ""
      classfications.foreach(element => {
        if(element.getAs(matchingElement).equals(matchingValue)){
//          value = element.getAs(returningValue)
          value = getSubsequentElements(returningValue, element)
        }
      })
    value
  })

  val findValue = udf((arr :mutable.WrappedArray[Row]) =>{
    var ans:String = ""
    val prioritySeq = Array[String] ( "TICKER", "ISIN")

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
