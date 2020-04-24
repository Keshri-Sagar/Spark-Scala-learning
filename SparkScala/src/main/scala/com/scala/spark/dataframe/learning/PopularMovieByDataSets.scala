package com.scala.spark.dataframe.learning

import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql._

object PopularMovieByDataSets {

  final case class MovieId(movieId: Int)
  def objectMapper(line: String): MovieId = {

    val fields = line.split("\t")
    val movieId: MovieId = MovieId(fields(1).toInt)

    return movieId
  }

  def main(array: Array[String]) = {

    org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)
    val sqlForPopularMovie = "SELECT movieId, COUNT(movieId) AS rating_count from popularMovie GROUP BY movieId ORDER BY rating_count"
    val sparkSession = SparkSession
      .builder()
      .appName("PopularMovieByDataSets")
      .master("local[*]")
      .getOrCreate()

    val fileContent = sparkSession.sparkContext.textFile("src/main/resources/movielens.txt")

    val movieRdd = fileContent.map(objectMapper)

    import sparkSession.implicits._

    val movieDs = movieRdd.toDS()

    movieDs.createOrReplaceTempView("popularMovie")

    movieDs.printSchema()

    println("by function syntax")
    //by function syntax
    val topmovieWithFunction = movieDs.groupBy("movieId").count().orderBy("count").cache()

    topmovieWithFunction.collect().foreach(println)

    //by spark sql syntax
    println("by spark sql syntax")
    val topmovieWithSql = sparkSession.sql(sqlForPopularMovie).cache()
    

    topmovieWithFunction.collect().foreach(println)

    sparkSession.close()

  }

}