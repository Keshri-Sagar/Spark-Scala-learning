package com.scala.spark.rdd.learning

import org.apache.spark._

object WordCount {

  def main(array: Array[String]) = {

    val sparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")

    val sparkContext = new SparkContext(sparkConf)

    val bookContent = sparkContext.textFile("src/main/resources/textFileForWCApp.txt")

    val words = bookContent.flatMap(x => x.toLowerCase() split ("\\W+"))

    /*    val wordCount = words.countByValue()

    wordCount.foreach(println)*/

    ///Second way to do this program by using reduceByKey()

    val map_word_rdd = words.map(x => (x, 1))

    val result = map_word_rdd.reduceByKey((x, y) => x + y)
    
    //(sagar,10),(sufi,5)

    val sorted_result_by_flip = result.map(x => (x._2, x._1)).sortByKey().collect()

    sorted_result_by_flip.foreach(println)

  }
}




/*

map- >    1-> 1

flatmap-> 1-> many          input-->  this is a dog and this can called as pet-> split (" ")  ->> Array(this, is, a, dog,and,this,can,be,called,as,pet)  <--output  .map(x=> (x,1)).reducebyKey((x,y)=>x+y)

                                            (this,1),(is,1),(a,1),(dog,1),(this,1),(can,1),(be,1),(called,1),)(as,1),(pet,1)
                                            
                                            
                                            (this,2),(is,6),(a,1),(dog,1),(can,1),(be,1),(called,3),(as,1),(pet,1)
                                            
                                            (is,6),(called,3),(this,2),(a,1),(dog,1),(can,1),(be,1),(as,1),(pet,1)
                                            
                                            
                                            
                                            
                                            

mapValues()--> 

flatMapvaluyes() ->>





*/









