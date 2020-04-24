package com.scala.spark.dataframe.learning

import org.apache.spark.sql._

object MultipleSparkSessions {


  def main(array: Array[String]) = {


    //Multiple Spark Session in one Driver Script.

    val sparkSession = SparkSession.builder().master("local[*]").appName("MultiPleSparkSession").getOrCreate()

    val sparkSession1 = SparkSession.builder().master("local[*]").appName("Multiple Sample 2").getOrCreate()


    val rddss1 = sparkSession.sparkContext.parallelize(Seq(1, 2, 3, 4))
    val rddss2 = sparkSession1.sparkContext.parallelize(Seq(1, 2, 3, 4))

    rddss1.collect.foreach(println)

    Thread.sleep(10000) /// Just to get some time between the execution
    rddss2.collect.foreach(println)

    //You can't have multiple SparkContext in a JVM but you can introduce multiple instance of SparkSession in one driver Script.

  }

}
