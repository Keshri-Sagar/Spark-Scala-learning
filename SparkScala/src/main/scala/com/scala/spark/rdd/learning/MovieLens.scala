package com.scala.spark.rdd.learning

import org.apache.spark._

object MovieLens {

  def parseline(line: String) = {

    val fields = line.split("\t")
    val movieId = fields(1).toInt
    val rating = fields(2).toInt

    (movieId)

  }

  def main(array: Array[String]) {

    val sconf = new SparkConf().setAppName("MovieLens").setMaster("local[*]")

    val sc = new SparkContext(sconf)

    val movieLensDataSet = sc.textFile("src/main/resources/movielens.txt")

    val parsedData = movieLensDataSet.map(parseline)

    val x = parsedData.map(x => (x, 1)).reduceByKey((x, y) => x + y).map(x => (x._2, x._1)).sortByKey().collect

    x.foreach(println)

  }

}