package com.test.sample

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object StreamingDataFrames {

  val spark = SparkSession.builder().appName("our first streams").master("local[2]").getOrCreate()
  spark.sparkContext.setLogLevel("error")

  def readFromSocket(): Unit ={

    //Created a dataframe by connecting spark to a socket
    val lines : DataFrame = spark.readStream.format("socket").option("host","localhost").option("port",12345).load()


    //Transform the dataframe
    val shortLines = lines.filter(length(col("value")) <= 5)

    //This is to tell between static vs streaming dataframe
    println("shortLines dataframe is streaming: "+shortLines.isStreaming)

    //Started action on dataframe by outputting each line on console
    //Started a streaming query
    val query = shortLines.writeStream.format("console").outputMode("append").start()



    //Wait for the streaming query to terminate
    query.awaitTermination()

  }


  def main(args: Array[String]): Unit ={
    readFromSocket()

  }

}
