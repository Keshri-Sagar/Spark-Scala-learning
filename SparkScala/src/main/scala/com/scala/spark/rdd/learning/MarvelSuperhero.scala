package com.scala.spark.rdd.learning

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object MarvelSuperhero {

  def parseline(line: String) = {

    val fields = line.split("\\s+")

    (fields(0).toInt, fields.length - 1)

    //(4567,14)

  }

  def parseName(line: String): Option[(Int, String)] = {

    val fields = line.split("\"")

    if (fields.length > 1) {
      return Some(fields(0).trim().toInt, fields(1).trim())
    } else
      return None

    //(4567,Marvel S)

  }

  def main(array: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)

    val sconf = new SparkConf().setAppName("MarvelSuperhero").setMaster("local[*]")

    val sc = new SparkContext(sconf)

    val marvelData = sc.textFile("src/main/resources/Marvel-graph.txt")

    val nameWithId = sc.textFile("src/main/resources/Marvel-names.txt")

    val nameAndIdMap = nameWithId.flatMap(parseName)

    //(4345,sagar)

    val idWithCountWithFrnds = marvelData.map(parseline)

    val totalConnections = idWithCountWithFrnds.reduceByKey((x, y) => x + y)

    //(4545,14)

    val sortedConnections = totalConnections.map(x => (x._2, x._1)).sortByKey()

    // sortedConnections.collect().foreach(println)

    val mostPopularSuperHero = sortedConnections.max()

    val superHeroWinner = nameAndIdMap.lookup(mostPopularSuperHero._2)(0)

    println(s"Most popular superhero is $superHeroWinner with  ${mostPopularSuperHero._1} cooccurences")

  }

}