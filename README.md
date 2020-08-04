
 ## Starting netcat client to post messages to socket using port 12345
    Use netcat
    nc -lk 12345

 ## Steps for starting docker containers for running kafka streaming job
    docker-compose up
    docker ps
    rockthejvm-sparkstreaming-kafka
    docker exec -it rockthejvm-sparkstreaming-kafka bash
    cd /opt/kafka_2.12-2.5.0/
    --Create topic
    bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic rockthejvm
    --Create kafka console producer to write messages to the topic
    bin/kafka-console-producer.sh --broker-list localhost:9092 --topic rockthejvm
    --Create kafka consumer console to read messages from the topic
    bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic rockthejvm --from-beginning

    Remember: In Kafka messages with same key will go to the same partition

 ## Spark Streaming Notes

    1. whenever you create dataframe using spark.readStream make sure you provide schema

    2. outputmode "append" and "update" is not supported on aggregations without watermarks.

    3. Aggregations with distinct are not supported by spark streaming.
        This is because spark will have to keep the entire state of the entire stream in order to fetch the distinct values in a stream.
        Example of aggregations:
        val lineCount = lines.selectExpr("count(*) as lineCount")

    4. Aggregations work at a micro batch level

    5. Some aggregations are not supported at all. e.g. sorting or chaining aggregations.

    6. If we are joining streaming dataframe with static dataframe  then the following joins are not permitted:
        Right Outer Join
        Full Outer Join
        Right Semi Join
        Right Anti Join

    7. Similarly if we are joining static dataframe with streaming dataframe then the following joins are not permitted:
        Left Outer Join
        Full Outer Join
        Left Semi Join
        Left Anti Join

    8. Since Spark 2.3 stream vs stream joins is supported in Spark

    9. Only outputMode "append" supported for stream vs stream join

    10. For stream vs stream joins inner joins are supported. left/right outer joins are supported, but must have watermarks. Full Outer joins are not supported.

 ## Discretized Streams(DStreams)

        Never ending sequence of RDDs
            Nodes clocks are synchronised
            batches are triggered at the same time in the cluster
            each batch is an RDD

        Essentially a distributed collection of elements of the same type
            functional operators e.g. map, filter, flatMap, reduce
            accessors to each RDD
            more advanced operators

        Needs a receiver to perform computations
            one receiver per Dstream
            fetches data from the source, sends to Spark, creates blocks
            is managed by the StreamingContext on the driver
            occupies one core on the machine

    ## Event Time
    The moment when the record was generated. It is set by the data generation system.
    This is different from processing time which is the time when the record arrives at Spark Engine.

    ## Watermark
    How far back we still consider records before dropping them
    Assume we have watermark of 10 minutes
    This means spark will drop older records then this watermark. In other words spark will only consider records that are at most 10 minutes old.
    At every window interval spark will recalculate the watermark by considering the max event time in that interval and deducting the watermark time interval from that.
    Ex: if max even time is 5:19 then 5:19 - 10 minutes will be 5:09 which will be the watermark value.
    Any record with event time lesser than this watermark value in the next sliding window interval will be dropped for computations.














