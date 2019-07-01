package com.dgmall.sparktest.kafka

import java.lang

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010._

/**
  * Spark Streaming 读取 Kafka数据 （求UV）
  * @Author: Cedaris
  * @Date: 2019/7/1 17:09
  */
object readKafka {

  def main(args: Array[String]): Unit = {
    //1、创建sparkConf并初始化sparkstreaming
    val sparkConf: SparkConf = new SparkConf().setAppName("kafka_spark")
      .setMaster("local[*]")
    val ssc: StreamingContext = new StreamingContext(sparkConf,Seconds(5))
    // 如果Spark批处理持续时间大于默认的Kafka心跳会话超时（30秒），请相应地增加heartbeat.interval.ms和session.timeout.ms。
    // 对于大于5分钟的批次，这将需要在代理上更改group.max.session.timeout.ms。

    //2、定义kafka参数
    val bootstrap: String = "psy831:9092,psy832:9092,psy833:9092"
    val topic: Array[String] = Array("hometown_headline")
    val consumergroup: String = "spark"

    //3、将kafka参数映射为map
    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> bootstrap,
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> consumergroup,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    /*
    The cache for consumers has a default maximum size of 64.
    If you expect to be handling more than (64 * number of executors) Kafka partitions,
     you can change this setting via spark.streaming.kafka.consumer.cache.maxCapacity
     */

    //4.通过KafkaUtil创建kafkaDSteam
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topic, kafkaParams)
    )

    val uv: DStream[(String, Long)] = kafkaStream.map {
      item => {
        val logs: Array[String] = item.value().split(",", 46)
        val userId: String = logs(0)
        (userId, 1L)
      }
    }.reduceByKey(_ + _)

    uv.cache()
    uv.print()

    ssc.start()
    ssc.awaitTermination()

  }
  case class userCount(userId:String,count:Long)
}
