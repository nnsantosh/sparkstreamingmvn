  package com.test.sample


  import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
  import org.apache.kafka.common.serialization.StringSerializer
  import org.apache.kafka.common.serialization.StringDeserializer
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
  import org.apache.spark.streaming.{Seconds, StreamingContext}

  object IntegratingKafkaDStreams {


    val spark = SparkSession.builder().appName("Integrating kafka DStreams").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("error")
    val ssc = new StreamingContext(spark.sparkContext,Seconds(1))

    val kafkaParams: Map[String,Object] = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.serializer" -> classOf[StringSerializer],
      "value.serializer"  -> classOf[StringSerializer],
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer"  -> classOf[StringDeserializer],
      "auto.offset.reset"  -> "latest",
      "enable.auto.commit"  -> false.asInstanceOf[Object]
    )

    val kafkaTopic = "rockthejvm"

    def readFromKafka(){
      val topics = Array(kafkaTopic)
      val kafkaDStream = KafkaUtils.createDirectStream(
        ssc,

        /**
          * Distributes the partitions evenly across the Spark cluster
          */
        LocationStrategies.PreferConsistent,

        /**
          *
          */
        ConsumerStrategies.Subscribe[String,String](topics, kafkaParams + ("group.id" -> "group1")))

      val processedStream = kafkaDStream.map(record => (record.key(), record.value()))
      processedStream.print()

      ssc.start()
      ssc.awaitTermination()
    }

    def writeToKafka(): Unit ={
      val inputData = ssc.socketTextStream("localhost",12345)
      val processedData = inputData.map(_.toUpperCase)
      processedData.foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          val kafkaHasMap  = new java.util.HashMap[String,Object]()
          kafkaParams.foreach{ pair =>
            kafkaHasMap.put(pair._1, pair._2)
          }
          //this producer is available on this executor
          val producer = new KafkaProducer[String,String](kafkaHasMap)
          partition.foreach(value => {
            val message = new ProducerRecord[String,String](kafkaTopic,null,value)
            producer.send(message)
          })
          producer.close()
        })
      })
      ssc.start()
      ssc.awaitTermination()
    }

    def main(args: Array[String]): Unit ={
      //readFromKafka()
      writeToKafka()
    }

  }
