package com.example.spark.utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.udf

import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

object UserDefinedFunctions {

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

  def toLowerCase = udf((value: String) => {
    value.toLowerCase
  })

  def findValue = udf((arr :mutable.WrappedArray[Row]) =>{
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
