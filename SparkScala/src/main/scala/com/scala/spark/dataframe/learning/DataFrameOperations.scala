package com.scala.spark.dataframe.learning

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

object DataFrameOperations {

  def defineSchema(): StructType = {

    //Way to define you own schema using spark Complex types. So that you can prevent Spark DF to parse it with it's own.
    val resultSchema = StructType(
      Array(
        StructField("DBN", StringType),
        StructField("School Name", StringType),
        StructField("Number of Test Takers", IntegerType),
        StructField("Critical Reading Mean", IntegerType),
        StructField("Mathematics Mean", IntegerType),
        StructField("Writing Mean", IntegerType)
      )
      // Schema of DF--> DBN|School Name|Number of Test Takers|Critical Reading Mean|Mathematics Mean|Writing Mean

    )
    return resultSchema
  }

  def main(array: Array[String]) = {


    val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)
    LogManager.getLogger("org").setLevel(Level.ERROR) // Set the log level to print only Errors.

    val spark = SparkSession
      .builder()
      .appName("DataFrameOperations")
      .master("local[*]")
      .getOrCreate()

    logger.info(s" Spark Version ${spark.version}")

    val csvFileDF = spark
      .read
      .schema(defineSchema()) // register your own schema while reading the csv file.
      .option("header", "true")
      .option("mode", "DROPMALFORMED") // option to drop teh rows which doesn't comply the schema.
      .csv("src/main/resources/results.csv")

    val dfWithoutNull = csvFileDF.na.drop() // Drop the Rows with null values.
    dfWithoutNull.printSchema()
    dfWithoutNull.show()

    dfWithoutNull.select("School Name".trim)
      .withColumn("NameAsSchoolColumn", dfWithoutNull.col("School Name")) // Derive new column from an existing column
      .write.partitionBy("DBN") // Partition by the column name 'DBN' and write it as Csv file.
      .mode(SaveMode.Overwrite)
      .csv("src/main/resources/SchoolName.csv")

    logger.info("Shutting Down the program..")

  }

}
