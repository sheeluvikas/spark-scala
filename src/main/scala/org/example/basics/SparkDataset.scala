package org.example.basics

import com.example.pojo.Student
import com.example.spark.utils.SparkUtils

object SparkDataset {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkUtils.buildSparkContext("Spark Dataset App")
    val df = sparkSession.read.json("src/main/resources/Student.json")
    df.write.mode("overwrite").parquet("src/main/resources/parquet")


    val studentDF = sparkSession.read.parquet("src/main/resources/parquet/")

    studentDF.show()

    import sparkSession.implicits._

    val student = studentDF.as[Student]

    student.filter(x => {
      x.age < 30
    }).show()

  }
}
