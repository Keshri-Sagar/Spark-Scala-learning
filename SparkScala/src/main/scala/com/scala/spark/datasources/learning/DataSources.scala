package com.scala.spark.datasources.learning

import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.sql.types._
import com.sagar.scala.dataframe.schema._

object DataSources {

  def main(array: Array[String]) = {

    val dfSchema = StructType(

      Array(
        StructField("Country", StringType),
        StructField("Indicator", StringType),
        StructField("Value", DoubleType),
        StructField("Year", IntegerType)))

    val spark = SparkSession.builder().appName("Data Sources").master("local[*]").getOrCreate()

    //Reading json dataframe  with mode and options
    val DFWithOptions = spark.read.format("json").schema(dfSchema).options(Map(

      "path" -> "src/main/resources/file.json"))
      .load()

    //Instead of defining the config with multiple options we can create options with Map.

    DFWithOptions.printSchema()
    DFWithOptions.show()

    //Writting json dataframe with mode and options
    DFWithOptions.write.format("json").mode(SaveMode.Overwrite).options(Map(
      "path" -> "src/main/resources/outputfile.json")).save()

    /*  DFWithOptions.groupBy("Indicator").count().orderBy("count").show()*/

  }

}

/*
 |-- Country: string (nullable = true)
 |-- Indicator: string (nullable = true)
 |-- Value: double (nullable = true)
 |-- Year: long (nullable = true)*/