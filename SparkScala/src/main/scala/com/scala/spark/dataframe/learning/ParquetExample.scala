package com.scala.spark.dataframe.learning

import org.apache.spark.sql.SparkSession._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object ParquetExample {

  def main(array: Array[String]) = {

    // org.apache.log4j.Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR)

    val spark = SparkSession.builder().appName(" Parquet learning").master("local[*]").getOrCreate()
    val data = Seq(("James ", "", "Smith", "36636", "M", 3000), ("Michael ", "Rose", "", "40288", "M", 4000), ("Robert ", "", "Williams", "42114", "M", 4000), ("Maria ", "Anne", "Jones", "39192", "F", 4000), ("Jen", "Mary", "Brown", "", "F", -1))

    // val columns = Seq("firstname", "middlename", "lastname", "dob", "gender", "salary")

    val schema = StructType(

      Array(
        StructField("firstname", StringType),
        StructField("middlename", StringType),
        StructField("lastname", StringType),
        StructField("dob", StringType),
        StructField("gender", StringType),
        StructField("salary", LongType)))

    import spark.implicits._
    val df = data.toDF("firstname", "middlename", "lastname", "dob", "gender", "salary")
    df.printSchema()
    df.show()

    df.write.partitionBy("salary", "gender").mode(SaveMode.Overwrite).parquet("C:/Users/sagar/Desktop/peopledetails.parquet")

    val parqDF = spark.read.parquet("src/main/resources/peopledetails.parquet")

    parqDF.createOrReplaceTempView("people")
    val sqlResult = spark.sql("SELECT * FROM people where salary>3000 and gender ='M'")

    sqlResult.show()

    val rdd = spark.sparkContext.parallelize(data)

    val DF = spark.createDataFrame(rdd)

    DF.show()

  }

}