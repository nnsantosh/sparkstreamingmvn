package com.test.sample

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreams {

  //In order to create DStreams you need SparkContext
  val spark = SparkSession.builder().appName("DStreams").master("local[2]").getOrCreate()

  //Spark Streaming Context is the entry point to DStreams
  //It takes spark context and batch interval as parameters

  val ssc = new StreamingContext(spark.sparkContext,Seconds(1))

  /**
    *
    * Define input sources by creating DStreams
    * Define transformations
    * Call an action on Dstreams
    * Start all computation with ssc.start()
    * no more computations after this point
    * await termination or stop the computation
    *
    *
    */

  def readDataFromSocket(): Unit ={
    val socketStream : DStream[String] = ssc.socketTextStream("localhost",12345)

    //Transformation
    val wordsStream = socketStream.flatMap(line => line.split(""))

    //To trigger action
    wordsStream.print()

    //You could also write ot file instead of printing

    ssc.start()
    ssc.awaitTermination()

  }

  def main(args: Array[String]): Unit ={
    readDataFromSocket()
  }



}
