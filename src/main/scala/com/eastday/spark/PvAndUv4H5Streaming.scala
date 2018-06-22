package com.eastday.spark

import com.eastday.conf.ConfigurationManager
import com.eastday.constract.Constract
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}


import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by admin on 2018/4/19.
 */
object PvAndUv4H5Streaming {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val kafkaParams =Map(
    "metadata.broker.list" -> "hadoop-sh1-core1:9092",
    "zookeeper.connect" -> "hadoop-sh1-master1:2181,hadoop-sh1-master2:2181,hadoop-sh1-core1:2181",
    "group.id" -> "test-consumer-group",
    "serializer.class" -> "kafka.serializer.StringEncoder",
    "auto.offset.reset" -> "largest"
  )
  val topics =ConfigurationManager.getString(Constract.KAFKA_TOPICS).split(",").toSet


//  def getStreaming(ssc:StreamingContext):DStream[(String,String)] ={
//    val offsetRanges  =null
//    offsetRanges match  {
//      case Some()
//    }
//
//  }

  def main(args: Array[String]) {
    val conf :SparkConf =new SparkConf()
        .setMaster("local")
        .setAppName("streaming test")
        .set(ConfigurationManager.getString(Constract.SPARK_DEFAULT_PARALLELISM),"3")
        .set("spark.executor.memory", "2g")
        .set("spark.executor.cores", "1")
        .set("spark.cores.max","3")
        .set("spark.testing.memory","471859200")
  //  val sc  =new SparkContext(conf)

    //val spark =SparkSession.builder().config(conf).getOrCreate()
    val ssc =new StreamingContext(conf,Seconds(3))
    //ssc.checkpoint("file:\\E:\\tool\\checkpoint")


    val  dStream :DStream[(String,String)]=
      KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
        ssc,
        kafkaParams,
        topics)
   val result =  dStream.map(_._2).transform(
      rdd =>{
        rdd.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)

      }
    )
    result.print()
//    val lines = ssc.socketTextStream("localhost", 9999)
//    val words = lines.flatMap(_.split(" "))
//    import org.apache.spark.streaming.StreamingContext._
//    // Count each word in each batch
//    val pairs = words.map(word => (word, 1))
//    val wordCounts = pairs.reduceByKey(_ + _)
//
//    // Print the first ten elements of each RDD generated in this DStream to the console
//    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

}
