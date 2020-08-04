package com.test.sample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ProcesingTimeWindows {

  val spark = SparkSession.builder().appName("Processing Time Windows").master("local[2]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  //Aggregate by event time which is the purchase time of incoming data
  //24 hour Window
  def aggregateByProcessingTime(): Unit ={
    val linesCharCountByWindowDF = spark.readStream.format("socket")
      .option("host","localhost")
      .option("port","12345").load()
      .select(col("value"),current_timestamp().as("processingTime"))
      .groupBy(window(col("processingTime"),"10 seconds").as("window"))
      .agg(sum(length(col("value"))).as("SumOfLengthOfInputStrings"))
        .select(col("window").getField("start").as("start"),
          col("window").getField("end").as("end"), col("SumOfLengthOfInputStrings") )


    linesCharCountByWindowDF.writeStream.format("console").outputMode("complete").start().awaitTermination()

    //windowByDay.writeStream.format("csv").option("checkpointLocation","outCheckPoint").start("output").awaitTermination()
  }

  def main(args: Array[String]): Unit ={
    aggregateByProcessingTime()

  }
}
