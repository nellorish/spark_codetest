package org.nellorem

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkScalaTest2 extends App {

  val spark = SparkSession.builder()
    .appName("CSV to DataFrame").config("spark.master", "local")
    .getOrCreate()

  // Define the path to your CSV file
  val member_months_path = "/Users/mnellore/Downloads/member_months.csv"

  val memeber_month_df = spark.read
    .option("header", "true") // If the CSV file has headers
    .option("inferSchema", "true") // Infer the data types of each column
    .csv(member_months_path)


  val memberMonthWithYearandMonth = memeber_month_df.withColumn("year", year(col("eligiblity_effective_date")))
    .withColumn("month", month(col("eligiblity_effective_date")))

// Question is little confusing if we need total number of member months per member per year. we need to include Year in calculation But since the result data
  // needs to be member_id and Month and number Months. Just do group by
  val result_set= memberMonthWithYearandMonth.groupBy("member_id","month").agg(sum("month").as("total_member_months"))

  val outputDir = "/Users/mnellore/hadoop2"
  result_set.write.json(outputDir)
}
