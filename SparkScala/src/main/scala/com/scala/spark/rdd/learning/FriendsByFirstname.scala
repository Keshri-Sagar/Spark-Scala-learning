package com.scala.spark.rdd.learning

import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.log4j._

object FriendsByFirstname {

  def parseline(line: String) = {
    val fields = line.split(",")
    val f_name = fields(1)
    val friends = fields(2).toInt
    (f_name, friends)

  }

  // output of parseline like (Sagar,20),(Ravi,30)//

  def main(array: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark_context = new SparkContext("local[*]", "FriendsByFirstName")

    val rdd = spark_context.textFile("src/main/resources/friendsbyage.txt")

    val paired_rdd = rdd.map(parseline)

    val mapvaluedrdd = paired_rdd.mapValues(x => (x, 1)).sortByKey() //output will be like (Leeta,(69,1))  //
    val reduced_rdd = mapvaluedrdd.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)) ///output will be like (Martok,(518,12)) , (Worf,(629,14))  ,(Rom,(596,12))
    //reduced_rdd.foreach(println)
    // mapvaluedrdd.foreach(println)

    val result = reduced_rdd.mapValues(x => x._1 / x._2)

    result.foreach(println)
  }

}