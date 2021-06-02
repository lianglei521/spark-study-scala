package cn.spark.study.streaming

import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2019/01/19.
  * 基于Kafka的receiver的实时WordCount程序
  */
object KafkaReceiverWordCount {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("基于Kafka的receiver的实时WordCount程序")
    //每5秒收集一个batch数据（一批rdd）
    val ssc = new StreamingContext(conf,Seconds(5))
    //指定到哪个topic，用几条线程读取数据
    val topicThreadMap: Map[String, Int] = Array(("WordCount",1)).toMap
    val index_lines: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
      ssc,//streamingContext实例
      "spark1:2181,spark2:2181,spark3:2181",//连接zookeeper
      "DefaultConsumerGroup",//消费组id
      topicThreadMap//指定到哪个topic下读取数据
    )
    val words:DStream[String] = index_lines.flatMap(tp =>tp._2.split(" "))
    val pairs:DStream[(String,Int)] = words.map((_,1))
    val wordCount:DStream[(String,Int)] = pairs.reduceByKey(_+_)

    wordCount.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
