package com.scala.spark.dataframe.learning

import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql._

import org.apache.spark.sql.types._
object ManualDFExample {

  def getSchema(): StructType = {

    val schema = StructType(
      Array(

        StructField("brand", StringType),
        StructField("Model", StringType),
        StructField("camera", IntegerType),
        StructField("Mfg Date", DateType)))

    return schema
    /* "brand", "Model", "camera", "date"*/
  }

  def main(array: Array[String]) = {

    val spark = SparkSession.builder().appName("ManualDFExample").master("local[*]").getOrCreate()
    //create manual rows from org.apache.spark.sql.Row._ package
    val row = Row(

      Array(
        ("Apple", "Iphone X", 12, "12-06-2006"),
        ("Samsung", "Galaxy 10", 23, "12-06-2016"),
        ("Apple", "Iphone X", 12, "12-06-2013"),
        ("Nokia", "N70", 15, "12-04-2015")))

    //create a seq of Mobile description and make a dataframe out of it.
    //(brand,Model,camera pixel,date)

    val mobileDetails = Seq(

      ("Apple", "Iphone X", 12, "12-06-2006"),
      ("Samsung", "Galaxy 10", 23, "12-06-2016"),
      ("Apple", "Iphone X", 12, "12-06-2013"),
      ("Nokia", "N70", 15, "12-04-2015"))

    val manualDF = spark.createDataFrame(mobileDetails)

    manualDF.printSchema()

    manualDF.show()

    import spark.implicits._

    //val dfWithRow = spark.createDataFrame(, getSchema())

    /*    val abc = Row("val1","val2")
val rdd = sc.parallelize(Seq(abc))
val rowRdd = rdd.map(row => Row(row.toSeq))
rowRdd: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row]*/

    val manualDFWithSchema = mobileDetails.toDF("brand", "Model", "camera", "date")

    manualDFWithSchema.printSchema()
    manualDFWithSchema.show()
  }

}