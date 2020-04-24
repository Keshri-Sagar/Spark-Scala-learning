package com.scala.spark.rdd.learning

import scala.io._
import org.apache.spark._
import java.nio.charset._
import org.apache.log4j._

object BroadcastExample {

  def parseline(line: String) = {

    val fields = line.split("\t")
    val movieId = fields(1).toInt
    val rating = fields(2).toInt
    (movieId)

  }

  def loadMovieNames(): Map[Int, String] = {

    implicit val codec = Codec("UTF-8")

    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movielens: Map[Int, String] = Map()

    var lines = Source.fromFile("src/main/resources/movieswithname.txt").getLines()

    for (line <- lines) {
      var fields = line.split("\\|")
      if (fields.length > 1) {
        movielens += (fields(0).toInt -> fields(1))
      }

    }
    return movielens

  }

  def main(array: Array[String]) {

    val sconf = new SparkConf().setAppName("MovieLens").setMaster("local[*]")

    val sc = new SparkContext(sconf)

    val movieNameDict = sc.broadcast(loadMovieNames)

    Logger.getLogger("BroadcastExample").setLevel(Level.INFO)

    val movieLensDataSet = sc.textFile("src/main/resources/movielens.txt")

    val parsedData = movieLensDataSet.map(parseline)

    val sortedMovieDetails = parsedData.map(x => (x, 1)).reduceByKey((x, y) => x + y).map(x => (x._2, x._1)).sortByKey().
      map(x => (movieNameDict.value(x._2), x._1))

    val result = sortedMovieDetails.collect()

    result.foreach(println)
  }

}