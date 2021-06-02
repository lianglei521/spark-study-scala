package cn.spark.study.streaming

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * author:liangsir 
  * qq:714628767
  * created 2019/01/19.
  * 基于Kafka的Direct方式的实时WordCount程序
  */
object KafkaDirectWordCount {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("基于Kafka的Direct方式的实时WordCount程序")
      .setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))
    val kafkaParams:Map[String,String] = Array(("metadata.broker.list","spark1:9092,spark2:9092,spark3:9092")).toMap
    val topics:Set[String] = Array("WordCount").toSet
    val lines:InputDStream[(String,String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams,topics)
    val words:DStream[String] = lines.flatMap(tp => tp._2.split(" "))
    val pairs:DStream[(String,Int)] = words.map(word =>(word,1))
    val wordCount:DStream[(String,Int)] = pairs.reduceByKey(_+_)
    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
