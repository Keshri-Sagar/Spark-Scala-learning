package com.scala.spark.dataframe.learning

import org.apache.spark._
import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql._

import java.util.Date

import org.slf4j.{ Logger, LoggerFactory }

object DataFrameWithoutSparkSql {

  case class Person(Id: Int, Name: String, Age: Int, NoOfFriends: Int, count: Int)

  val splitBy = ","
  def defineColumnsForDataSet(line: String): Person = {

    var counter = 1
    val fields = line.split(splitBy)

    val person: Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt, counter)
    counter += 1
    return person
  }

  def main(array: Array[String]) = {

    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR) //setting log level to Error to avoiding unnecessary logs to the console

    val logger: Logger = LoggerFactory.getLogger(DataFrameWithoutSparkSql.getClass)

    val sparkSession = SparkSession.builder().appName("DataFrameWithoutSparkSql").master("local[*]").getOrCreate()

    val fakeFriends = sparkSession.sparkContext.textFile("src/main/resources/fakefriends.csv")

    val fakeFriendsWithSchema = fakeFriends.map(defineColumnsForDataSet)

    import sparkSession.implicits._
    logger.info("Conveting Rdd to DataSets")

    val fakeFriendsDS = fakeFriendsWithSchema.toDS().cache()

    fakeFriendsDS.printSchema()
    logger.info("Name Starts with S")

    fakeFriendsDS.filter(fakeFriendsDS("Name").startsWith("S")).show()

    Thread.sleep(1000)

    logger.info("Age greater than 21")

    fakeFriendsDS.filter($"Age" < 21).orderBy("Age").show()
    Thread.sleep(1000)

    logger.info("Age between 10 and 20")

    fakeFriendsDS.filter($"Age" > 10 && $"Age" < 50).orderBy("Age").show()
    Thread.sleep(1000)
    logger.info("Group By Age")

    fakeFriendsDS.groupBy($"Age").count().show()
    Thread.sleep(1000)
    logger.info("Age + 10 ")

    fakeFriendsDS.select(fakeFriendsDS("Name"), fakeFriendsDS("Age") + 10).show()
    Thread.sleep(1000)
    sparkSession.close()

  }
}