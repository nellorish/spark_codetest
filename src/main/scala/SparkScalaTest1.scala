package org.nellorem

import org.apache.spark.sql.SparkSession

object SparkScalaTest1 extends App{

  // Create a SparkSession
  val spark = SparkSession.builder()
    .appName("CSV to DataFrame").config("spark.master","local")
    .getOrCreate()

  // Define the path to your CSV file
  val member_months_path = "/Users/mnellore/Downloads/member_months.csv"
  val member_eligibility_path = "/Users/mnellore/Downloads/member_eligibility.csv"

  // Load the CSV file into a DataFrame
  val memeber_month_df = spark.read
    .option("header", "true") // If the CSV file has headers
    .option("inferSchema", "true") // Infer the data types of each column
    .csv(member_months_path).withColumnRenamed("member_id", "me_member_id")

  // Load the CSV file into a DataFrame
  val memebers_elgibility_df = spark.read
    .option("header", "true") // If the CSV file has headers
    .option("inferSchema", "true") // Infer the data types of each column
    .csv(member_eligibility_path)

  //memeber_month_df.show()

  // Aggreagte by MemberID and Count the months member is active
  val aggregatedElgibityDF = memeber_month_df.groupBy("me_member_id").count().withColumnRenamed("count","member_months")

  //Inner join to get Name and other details of the Member
  val result_df = memebers_elgibility_df.join(aggregatedElgibityDF, memebers_elgibility_df("member_id")=== aggregatedElgibityDF("me_member_id"),"inner").drop("me_member_id")



  val outputDir= "/Users/mnellore/hadoop"
  result_df.write.partitionBy("member_id").json(outputDir)

   //result_df.show();

}
