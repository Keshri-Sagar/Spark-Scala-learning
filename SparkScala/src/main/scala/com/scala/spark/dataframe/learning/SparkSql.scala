package com.scala.spark.dataframe.learning

import org.apache.spark._
import org.apache.spark.sql._

import org.slf4j.{ Logger, LoggerFactory }

object SparkSql {

  //Delimiter by which the file is delimited by
  final val splitBy = ","

  //Case class to map the unstructured data to Structured columns
  case class Person(ID: Int, Name: String, Age: Int, NumOfFriends: Int)

  def objectMapper(line: String): Person = {

    val fields = line.split(splitBy)

    val person: Person = Person(fields(0).trim().toInt, fields(1).trim(), fields(2).trim().toInt, fields(3).trim().toInt)
    return person

  }

  def main(array: Array[String]) = {

    def logger: Logger = LoggerFactory.getLogger(SparkSql.getClass)
    //def log : Logger = LoggerFactory.getLogger( SparkSql.getClass )

    val sqlStatement = "SELECT Name FROM people WHERE Age BETWEEN 13 AND 19"

    val sparkSession = SparkSession
      .builder
      .appName("SparkSql")
      .master("local[*]")
      .getOrCreate()

    logger.info("SparkSession Create by utilizing the all cores of local system ")

    logger.info("Reading the file through SparkSession")
    val file = sparkSession.sparkContext.textFile("src/main/resources/fakefriends.csv")

    val persons = file.map(objectMapper)

    import sparkSession.implicits._ // this import is for the conversion of rdd to DataSet

    logger.info("Converting rdd to DataSet")
    val peopleDataFrame = persons.toDS() //convert the rdd to DataSet

    peopleDataFrame.printSchema().toString()

    logger.info("Creating the temp table people to run spark sql over it")

    peopleDataFrame.createOrReplaceTempView("people") // This will create a temporary table in Memory to act it like a relational table.

    val teenagers = sparkSession.sql(sqlStatement).sort("Name")
    
    teenagers.collect.foreach(println)

    logger.info("Please find the list of teenagers in the fakefriends dataset!!")

    logger.info("Closing the Spark Session!!")

    sparkSession.stop()

    logger.info("Shutting down the program !!")
  }

}