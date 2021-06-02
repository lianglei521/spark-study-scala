package cn.spark.study.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2019/01/18.
  * 基于socket数据源的实时WordCount程序
  */
object SocketWordCount {
  //屏蔽掉日志
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("实时WordCount程序")
      .setMaster("local[2]")//此处必须设置大于2个线程，一个线程接受数据，一个处理数据
    //创建流对象，每2秒收集一个batch
    val ssc: StreamingContext = new StreamingContext(conf,Seconds(2))
    //nc -lk 9000 向9000端口写入数据，然后从那个网络端口读取数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("spark1",9999)
    val words:DStream[String] = lines.flatMap(_.split(" "))
    val pairs:DStream[(String,Int)] = words.map((_,1))
    val wordCount:DStream[(String,Int)] = pairs.reduceByKey(_+_)
    wordCount.print()
    Thread.sleep(5000)

    //启动程序
    ssc.start()
    //阻塞等待
    ssc.awaitTermination()
    //释放资源
    ssc.stop()
  }
}
