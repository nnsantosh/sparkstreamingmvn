package com.test.sample

import java.io.PrintStream
import java.net.ServerSocket
import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

import scala.concurrent.duration._

object Watermarks {

  val spark = SparkSession.builder().appName("Late Data with Watermarks").master("local[2]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  //3000,blue

  import spark.implicits._

  def debugQuery(query: StreamingQuery): Unit = {
    //for debugging
    new Thread(() => {
      (1 to 100).foreach { i =>
        Thread.sleep(1000)
        val queryEventTime = if (query.lastProgress == null) "[]" else query.lastProgress.eventTime.toString
        println(s"$i: $queryEventTime")
      }
    })

  }

  def testWaterMark(): Unit = {
    val dataDF = spark.readStream.format("socket").option("host", "localhost").option("port", 12345)
      .load().as[String].map { line =>
      val tokens = line.split(",")
      val timestamp = new Timestamp(tokens(0).toLong)
      val data = tokens(1)
      (timestamp, data)
    }.toDF("created", "color")

    val watermarkedDF = dataDF.withWatermark("created", "2 seconds")
      .groupBy(window(col("created"), "2 seconds"), col("color"))
      .count().selectExpr("window.*", "color", "count")

    val query = watermarkedDF.writeStream.format("console").outputMode("append").trigger(Trigger.ProcessingTime(2.seconds)).start()

    debugQuery(query)

    query.awaitTermination()

  }



  object DataSender{
    val serverSocket = new ServerSocket(12345)
    val socket = serverSocket.accept() //blocking call
    val printer = new PrintStream(socket.getOutputStream)
    println("socket accepted")

    def example1(): Unit ={
      Thread.sleep(7000)
      printer.println("7000,blue")
      Thread.sleep(1000)
      printer.println("8000,green")
      Thread.sleep(4000)
      printer.println("40000,blue")
      Thread.sleep(1000)

    }

    def main(args: Array[String]): Unit = {
      example1()
    }


  }

}
