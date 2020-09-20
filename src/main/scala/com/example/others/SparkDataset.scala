package com.example.others

import com.example.pojo.Student
import com.example.spark.utils.SparkUtils

object SparkDataset {

  def main(args: Array[String]): Unit = {
    /** Get the spark session */
    val sparkSession = SparkUtils.buildSparkContext("Spark Dataset App", null)

    /** get the dataframe from the json object. */
    val df = sparkSession.read.json("src/main/resources/Student.json")

    /** write the above dataframe into the parquet file */
    df.write.mode("overwrite").parquet("src/main/resources/parquet")


    /** Now read the above parquet file in a dataframe */
    val studentDF = sparkSession.read.parquet("src/main/resources/parquet/")

    println("****** Showing the student Dataframe from Parquet file :")
    studentDF.show()

    import sparkSession.implicits._

    /** Convert the above dataframe in to a dataset of Student type. */
    val student = studentDF.as[Student] /** The Student should be case class */

    println("****** Printing the Student dataset :")
    student.filter(x => {
      x.age < 30
    }).show()

  }
}
