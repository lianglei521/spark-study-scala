package cn.spark.study.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Durations, Seconds, StreamingContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2019/01/20.
  * 基于滑动窗口的热点搜索词实时统计
  */
object WindowHotWord {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("基于滑动窗口的热点搜索词实时统计")
      .setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(1))
    val searchLog:ReceiverInputDStream[String] = ssc.socketTextStream("spark1",9999)
    val searchWord:DStream[String] = searchLog.map(line => line.split(" ")(1))
    val wordAndOne:DStream[(String,Int)] = searchWord.map(word =>(word,1))
    //通过窗口函数进行聚合
    //匿名函数进行聚合，第二个参数是窗口大小60秒，第三个参数是窗口间隔10秒
    val wordCount:DStream[(String,Int)] = wordAndOne.reduceByKeyAndWindow((a:Int,b:Int) => a+b ,Seconds(60),Seconds(10))
    //通过transformToPair对窗口里的RDD进行需求操作
    val wordCountDS:DStream[(String,Int)] = wordCount.transform(wordCountRDD =>{
      //key,value位置转化
      val countWord = wordCountRDD.map(tp => (tp._2,tp._1))
      //进行降序排序
      val sortedCountWord = countWord.sortByKey(false)
      //key，value位置再次互换
      val sortedWordCount = sortedCountWord.map(tp =>(tp._2,tp._1))
      //取top3,并打印
      sortedWordCount.take(3).foreach(println)
      //返回此时这个窗口函数的数据
      wordCountRDD
    })
    //打印每个滑动窗口里的RDD数据
    wordCountDS.print()
    //启动程序
    ssc.start()
    //阻塞等待
    ssc.awaitTermination()
    ssc.stop()
  }
}
