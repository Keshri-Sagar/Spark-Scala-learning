package com.scala.spark.dataframe.learning


import org.apache.spark.sql._

object AvroFileFormat {


  def main(array: Array[String]) = {

    val sparkSession = SparkSession.builder().master("local[*]").appName("Avro File").getOrCreate()

    println(sparkSession.version)

    //You have to add a dependency in your pom.xml file for avro support from dataBricks.

    val avroDF = sparkSession.read.format("com.databricks.spark.avro").load("C:/Users/sagar/Desktop/userdata1.avro")

    avroDF.printSchema()

    avroDF.show()


  }

}
