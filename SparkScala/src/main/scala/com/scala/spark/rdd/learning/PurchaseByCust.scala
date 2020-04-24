package com.scala.spark.rdd.learning

import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object PurchaseByCust {

  def parseline(line: String) = {

    val fields = line.split(",")
    val cust_id = fields(0).toInt
    val amt = fields(2).toFloat
    (cust_id, amt)

  }

  def main(array: Array[String]) = {
    val sconf = new SparkConf().setAppName("PurchaseByCust").setMaster("local[*]")
    val sc = new SparkContext(sconf)

    val purchase = sc.textFile("src/main/resources/custOrders.txt")

    val cust_amt = purchase.map(parseline)
    val pair_cust_amt = cust_amt.reduceByKey((amt1, amt2) => amt1 + amt2).map(x => (x._2, x._1)).sortByKey().collect()

    pair_cust_amt.foreach(println)
  }
}