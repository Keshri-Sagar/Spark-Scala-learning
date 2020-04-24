package com.scala.spark.rdd.learning

import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.log4j._

object TempByLocation {

  def parseline(line: String) = {

    // if (line.contains("TMIN")) {

    val fields = line.split(",")
    val station_id = fields(0)
    val field_type = fields(2)
    val temprature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (station_id, field_type, temprature)

    //  }

  }

  def main(array: Array[String]) = {

    Logger.getLogger("TempByLocation").setLevel(Level.ERROR)

    val spark_conf = new SparkConf().setAppName("TempByLocation").setMaster("local")
    val spark_context = new SparkContext(spark_conf)

    val data = spark_context.textFile("src/main/resources/Min_Temp.txt")

    //val parsed_data = data.map(line => (line.split(",")(0), line.split(",")(2), line.split(",")(3)))

    val parsed_data = data.map(parseline)

    val tmin_data = parsed_data.filter(x => x._2 == "TMIN")

    val tmax_data = parsed_data.filter(x => x._2 == "TMAX")

    // tmin_data.collect().foreach(println)

    val tmin_field_removed = tmin_data.map(x => (x._1, x._3.toFloat))
    val tmax_field_removed = tmax_data.map(x => (x._1, x._3.toFloat))

    val result_for_min_temp = tmin_field_removed.reduceByKey((x, y) => Math.min(x, y)).collect
    val result_for_max_temp = tmax_field_removed.reduceByKey((x, y) => Math.max(x, y)).collect

    for (res <- result_for_min_temp.sorted) {
      val station_id = res._1
      val temp_in_farenhiet = res._2

      println(s"temprature for station id $station_id is $temp_in_farenhiet")

    }

    println("================================================================================================================")
    
    for (res <- result_for_max_temp.sorted) {
      val station_id = res._1
      val temp_in_farenhiet = res._2

      println(s"temprature for station id $station_id is $temp_in_farenhiet")

    }

  }

}