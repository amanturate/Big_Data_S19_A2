package Assignment_19

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Assignment_19_2 extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val spark = SparkSession.builder
    .master("local")
    .appName("example")
    .config("spark.sql.warehouse.dir","C://ACADGILD")
    .getOrCreate()

  // A CSV dataset is pointed to by path.
  // The path can be either a single text file or a directory storing text files.
  val dataset_1 = spark.sqlContext.read.format("csv").option("header", "true")
    .option("inferSchema", true)
    .load("C:/ACADGILD/Big Data/SESSION_19/Sports_data.txt").toDF()


  // Register this DataFrame as a table.
  dataset_1.createOrReplaceTempView("sports")

  //--------------------------------------------------------------------------------------------------------------------
  //------------------------------------------ PROBLEM 1 ---------------------------------------------------------------
  //--------------------------------------------------------------------------------------------------------------------

//  UDFs transform values from a single row within a table to produce a single corresponding output value per row.

  def alter_name = org.apache.spark.sql.functions.udf((first_name:String,last_name:String) =>
                      {"Mr." + first_name.substring(0,2) + " " + last_name})

//  withColumn returns a new SparkDataFrame with an added column, typically after performing a column operation.
  dataset_1.withColumn("Modified_name", alter_name(dataset_1("firstname"),dataset_1("lastname"))).show()

  //--------------------------------------------------------------------------------------------------------------------
  //------------------------------------------ PROBLEM 2 ---------------------------------------------------------------
  //--------------------------------------------------------------------------------------------------------------------
  def ranking = org.apache.spark.sql.functions.udf( (medal:String,age:Int) =>{medal match {
    case ("gold")    if (age >= 32) => "PRO"
    case ("gold")    if (age <= 31) => "AMATEUR"
    case ("silver")  if (age >= 32) => "EXPERT"
    case ("silver")  if (age <= 31) => "ROOKIE"
  }
  })

  dataset_1.withColumn("ranking", ranking(dataset_1("medal_type"),dataset_1("age"))).show()


}
