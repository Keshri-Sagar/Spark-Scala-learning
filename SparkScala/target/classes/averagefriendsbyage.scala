
import org.apache.spark._
import org.apache.spark.SparkContext._
import java.util.{ Date }

object TransformationWithSpark {

  def main(array: Array[String]) = {
    val conf = new SparkConf().setAppName("testApp").setMaster("local")
    val sparkContext = new SparkContext(conf)

    val rdd = sparkContext.textFile("C:/Users/sagar/Desktop/friendsbyage.txt")

    val avg_no_of_friends_by_age = rdd.map(lines => (lines.split(",")(2).
      toInt, lines.split(",")(3).toInt)).
      mapValues(x => (x, 1)).
      reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).
      mapValues(x => x._1 / x._2)

    avg_no_of_friends_by_age.foreach(println)

    print("Process Finished with exit code 0. Shuting down the execution process at")

  }

  /*  def parseline(lines: String): Unit = {
    val records = lines.split(",")
    val age = records(2)
    val friends = records(3)
    mapValues(x => (x, 1)).
      reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).
      mapValues(x => x._1 / x._2)

  }*/

}