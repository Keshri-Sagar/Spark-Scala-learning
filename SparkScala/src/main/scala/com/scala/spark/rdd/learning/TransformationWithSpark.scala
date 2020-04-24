
package com.scala.spark.rdd.learning

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object TransformationWithSpark {

  def main(array: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("TransformationWithSpark").setMaster("local")
    val sparkContext = new SparkContext(conf)

    val rdd = sparkContext.textFile("src/main/resources/friendsbyage.txt")

    val avg_no_of_friends_by_age = rdd.map(lines => (lines.split(",")(2).
      toInt, lines.split(",")(3).toInt)).
      mapValues(x => (x, 1)).
      reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).
      mapValues(x => x._1 / x._2)

    avg_no_of_friends_by_age.foreach(println)

    print("Process Finished with exit code 0. Shuting down the execution process at")

  }

}