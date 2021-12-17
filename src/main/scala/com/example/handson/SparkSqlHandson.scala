package com.example.handson

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, countDistinct}


/**
 * https://towardsdatascience.com/six-spark-exercises-to-rule-them-all-242445b24565
 */
object SparkSqlHandson {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Session Application")
      .master("local")
      .getOrCreate()

    //************************************************************************************
    val sparkRead = spark.read
      .option("sep", ",")
      .option("header", "true")
      .option("inferSchema", "true")

    val sellersDF = sparkRead.csv("src/main/resources/data/sellers.csv")
    val salesDF = sparkRead.csv("src/main/resources/data/sales.csv")
    val productsDF = sparkRead.csv("src/main/resources/data/products.csv")

/*
    sellersDF.show(false)
    salesDF.show(false)
    productsDF.show(false)
*/

    /** Find out how many orders, how many products and how many sellers are in the data.
        How many products have been sold at least once? Which is the product contained
        in more orders?
     */

    println("******* Products in the data : " + productsDF.count())
    println("******* sellers in the data : " + sellersDF.count())

    val soldProductsDF = productsDF
      .join(salesDF, productsDF("product_id") === salesDF("product_id"), "right")
      .select(productsDF("product_id")).distinct()

    print("products sold at least once : " + soldProductsDF.count())

    /** print("******* Products in the data : " + productsDF.count()) */


    /** How many distinct products have been sold in each day? */
    val df = salesDF.select("product_id", "date").groupBy("date").count()
    df.show(false)

  }
}
