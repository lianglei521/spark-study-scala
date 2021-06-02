package cn.spark.study.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2019/01/19.
  * 基于UpdateStateByKey算子实现缓存机制的实时WordCount程序
  */
object UpdateStateByKeyWordCount {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("UpdateStateByKeyWordCount")
      .setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))
    //开启checkpoint机制
    ssc.checkpoint("hdfs://spark1:9000/wordcount_checkpoint")
    val lines:ReceiverInputDStream[String] = ssc.socketTextStream("spark1",9999)
    val words:DStream[String] = lines.flatMap(line => line.split(" "))
    val pairs:DStream[(String,Int)] = words.map(word =>(word,1))
    val wordCount:DStream[(String,Int)] = pairs.updateStateByKey((values:Seq[Int],state:Option[Int]) =>{
      var newState = state.getOrElse(0)
      for(value <- values){
        newState += value
      }
      Option[Int](newState)
    })

    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
