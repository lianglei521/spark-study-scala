package cn.spark.study.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * author:liangsir 
  * qq:714628767
  * created 2019/01/18.
  * 基于HDFS文件目录的WordCount程序
  */
object HDFSWordCount {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("HDFSWordCount")
    val ssc:StreamingContext = new StreamingContext(conf,Seconds(2))
    //这个程序必须提交到spark集群上运行，因为读的是hdfs文件
    val lines:DStream[String] = ssc.textFileStream("hdfs://spark1:9000/wordcount_dir")
    val words:DStream[String] = lines.flatMap(_.split(" "))
    val pairs:DStream[(String,Int)] = words.map((_,1))
    val wordCount:DStream[(String,Int)] = pairs.reduceByKey(_+_)

    wordCount.print()
    Thread.sleep(5000)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }
}
