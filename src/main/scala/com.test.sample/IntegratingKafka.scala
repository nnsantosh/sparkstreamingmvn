package com.test.sample


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object IntegratingKafka {

  val spark = SparkSession.builder().appName("Integrating kafka").master("local[2]").getOrCreate()
  spark.sparkContext.setLogLevel("error")

  def readFromKafka(): Unit ={
    val kafkaDF = spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","rockthejvm").load()
    val modifiedDF = kafkaDF.select(col("topic"),expr("cast(value as string) as actualValue"))
    modifiedDF.writeStream.format("console").outputMode("append").start().awaitTermination()
  }

  def writeToKafka(): Unit ={
    val carsSchema = spark.read.json("src/main/resources/data/cars/cars.json").schema
    val carsDF = spark.readStream.schema(carsSchema).json("src/main/resources/data/cars")

    val carsKafkaDF = carsDF.selectExpr("upper(Name) as key","Name as value")

    carsKafkaDF.writeStream.format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("topic","rockthejvm")
      .option("checkpointLocation","checkpoints")
      .start().awaitTermination()
  }

  def main(args: Array[String]): Unit ={
    //readFromKafka()
    writeToKafka()
  }

}
