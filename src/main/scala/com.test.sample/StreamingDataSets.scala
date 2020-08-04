package com.test.sample


import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, length}



object StreamingDataSets {

  val spark = SparkSession.builder().appName("our first streams").master("local[2]").getOrCreate()
  spark.sparkContext.setLogLevel("error")

  case class CustomMessage(value:String)


  def readFromSocket(): Unit ={

    //Created a dataframe by connecting spark to a socket
    val lines : DataFrame = spark.readStream.format("socket").option("host","localhost").option("port",12345).load()
    import spark.implicits._
    //Convert the DataFrame to Dataset by specifying a case class
    val shortLines: Dataset[CustomMessage] = lines.filter(length(col("value")) <= 5).as[CustomMessage]

    //This is to tell between static vs streaming dataframe
    println("shortLines dataset is streaming: "+shortLines.isStreaming)

    //Started action on dataset by outputting each line on console
    //Started a streaming query
    val query = shortLines.writeStream.format("console").outputMode("append").start()

    //Wait for the streaming query to terminate
    query.awaitTermination()

  }


  def main(args: Array[String]): Unit ={
    readFromSocket()

  }

}
