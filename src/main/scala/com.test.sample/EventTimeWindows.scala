package com.test.sample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object EventTimeWindows {

  val spark = SparkSession.builder().appName("Event Time Windows").master("local[2]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val onlinePurchaseSchema = StructType(Array(
    StructField("id",StringType),
    StructField("time",TimestampType),
    StructField("item",StringType),
    StructField("quantity",IntegerType)))

    def readPurchasesFromSocket() ={
      spark.readStream.format("socket").option("host","localhost").option("port","12345").load()
        .select(from_json(col("value"),onlinePurchaseSchema).as("purchase")).selectExpr("purchase.*")

    }

  //Aggregate by event time which is the purchase time of incoming data
  //24 hour Window
  def aggregatePurchasesBySlidingWindow(): Unit ={
    val purchaseDF = readPurchasesFromSocket()
    val windowByDay = purchaseDF
      .groupBy(window(col("time"),"1 minute","10 seconds").as("time")) //struct column has fields {start,end}
      .agg(sum("quantity").as("totalQuantity"))
        .select(col("time").getField("start").as("start"), col("time").getField("end").as("end"), col("totalQuantity"))

    windowByDay.writeStream.format("console").outputMode("complete").start().awaitTermination()

    //windowByDay.writeStream.format("csv").option("checkpointLocation","outCheckPoint").start("output").awaitTermination()
  }

  def main(args: Array[String]): Unit ={
    aggregatePurchasesBySlidingWindow()

  }
}
