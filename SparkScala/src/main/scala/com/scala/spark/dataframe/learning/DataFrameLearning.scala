package com.scala.spark.dataframe.learning


import org.apache.spark.sql._
import org.slf4j._
import org.apache.spark.sql.types._
object DataFarmeLearning {

  def defineDFSchema(): StructType = {

    //this is how you define your schema manually if you don't want spark to parse your data internally
    //you can register by using .schema option available while reading the file.
    val structType = StructType(

      Array(
        StructField("NAME", StringType, true), // by default the nullable filed value is set to true.
        StructField("ID", IntegerType), // need not to mention true explicitly.
        StructField("ADD", StringType, true),
        StructField("PIN", IntegerType, true)))

    return structType
  }

  def main(array: Array[String]): Unit = {

    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)

    val logger: Logger = LoggerFactory.getLogger(DataFarmeLearning.getClass)
    val spark = SparkSession.builder().appName("DataFrameLearning").master("local[*]").getOrCreate()

    val dfWithInferSchema = spark.read.option("header", "true").option("inferschema", "true").csv("src/main/resources/stuct.csv")
    dfWithInferSchema.printSchema()
    dfWithInferSchema.show()

    logger.info("==========================================================================================================================================================")

    val dfWithSchema = spark.read.format("csv").option("header", "true").schema(defineDFSchema()).load("src/main/resources/stuct.csv")

    dfWithSchema.printSchema()
    dfWithSchema.show()

  }

}