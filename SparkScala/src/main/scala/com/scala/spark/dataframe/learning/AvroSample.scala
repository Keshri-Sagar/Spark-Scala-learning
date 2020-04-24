package com.scala.spark.dataframe.learning

import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql._

object AvroSample {

  def main(array: Array[String]) = {

    val spark = SparkSession.builder().appName("Avro Sample").master("local[*]").getOrCreate()

    val df = spark.read.json("C:/Users/sagar/Desktop/file.json")

    df.printSchema()
    df.show()

    df.write.format("avro").save("src/main/resources/file_avro.avro")

  }

}