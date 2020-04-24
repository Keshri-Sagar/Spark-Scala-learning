package com.scala.spark.dataframe.learning

import org.apache.spark.sql.Row._
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object FixedLengthFileDF {

  //define structure for fixed length file through String utils

  def formatData(line: String): Row = {

    // val columnArray = new Array[String](4)

    /* columnArray(0) = line.substring(0, 3)
    columnArray(1) = line.substring(3, 13)
    columnArray(2) = line.substring(13, 18)
    columnArray(3) = line.substring(18, 22)*/
    val field_0 = line.substring(0, 3).trim().toInt
    val field_1 = line.substring(3, 13).trim().toString()
    val field_2 = line.substring(13, 18).trim().toBoolean
    val field_3 = line.substring(18, 22).trim().toFloat

    //Row.fromSeq(columnArray)

    Row(field_0, field_1, field_2, field_3)

    /* Row.fromSeq() will not work here.

//If it is called on the array of String, the resulting Row is the row of String's. Which does not match the type of the 1st column in the schema (bigint or int).*/

  }

  def getSchema(): StructType = {

    val schema = StructType(
      Array(
        StructField("ID", IntegerType),
        StructField("Name", StringType),
        StructField("Active", BooleanType),
        StructField("Humidity", FloatType)))
    return schema
  }
  def main(array: Array[String]) = {

    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)

    val filePath = "src/main/resources/flexiFile.txt"
    val spark = SparkSession.builder().appName("FlexiFile").master("local[*]").getOrCreate()

    import spark.implicits._
    val file = spark.createDataFrame(spark.sparkContext.textFile(filePath).map(x => formatData(x)), getSchema())
    file.printSchema()

    file.show()

  }

}