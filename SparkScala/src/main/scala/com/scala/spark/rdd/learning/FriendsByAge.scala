package com.scala.spark.rdd.learning

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object FriendsByAge {

  def parseline(line: String) = {
    val fields = line.split(",")
    val age = fields(2).toInt
    val friends = fields(3).toInt
    (age, friends)
  }

  def main(array: Array[String]) = {

    Logger.getLogger("FriendsByAge").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("FriendsByAge").setMaster("local")
    val spark_context = new SparkContext(conf)
    val rdd_friends_data = spark_context.textFile("src/main/resources/friendsbyage.txt")
    val paired_rdd = rdd_friends_data.map(parseline)

    val paired_rdd1 = paired_rdd.mapValues(x => (x, 1))
    val reduced_rdd = paired_rdd1.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => x._1 / x._2).sortBy(_._2)

    reduced_rdd.collect().foreach(println)
  }

}