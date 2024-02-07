package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, expr}

object Lab2{
def main(args: Array[String]) {

// 创建一个 SparkSession
val spark = SparkSession.builder
  .appName("Lab2")
  .getOrCreate()

// 读取数据源创建 DataFrame
val df = spark.read
  .format("csv")
  .option("header", "true")
  .load("./data/Divvy_Trips_2015-Q1.csv")

// 打印 DataFrame 的 Schema
df.printSchema()
val schema = StructType(Array(
      StructField("trip_id", StringType, true),
      StructField("starttime", StringType, true),
      StructField("stoptime", StringType, true),
      StructField("bikeid", StringType, true),
      StructField("tripduration", StringType, true),
      StructField("from_station_id", StringType, true),
      StructField("from_station_name", StringType, true),
      StructField("to_station_id", StringType, true),
      StructField("to_station_name", StringType, true),
      StructField("usertype", StringType, true),
      StructField("gender", StringType, true),
      StructField("birthyear", StringType, true)
    ))

    // Read the CSV file into a DataFrame with the specified schema
    val df_with_schema = spark.read
      .format("csv")
      .option("header", "true")
      .schema(schema)
      .load("./data/Divvy_Trips_2015-Q1.csv")

    // Show the DataFrame
    df_with_schema.show()

    // Print the number of rows
    println("\nNumber of Rows:", df_with_schema.count())

    // Stop the SparkSession
    spark.stop()

}
}
