package cn.spark.study.streaming


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2019/01/20.
  * 基于transform实时广告计费黑名单过滤
  */
object TransformBlackList {
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("TransformBlackList")
      .setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(5))
    //造一份黑名单数据并转化为RDD----------数据格式为username + flag
    val blackList = Array(("tom",true))
    val blackListRDD:RDD[(String,Boolean)] = ssc.sparkContext.parallelize(blackList)
    //读取用户广告点击日志，创建流数据集------日志格式为 date + user
    val ADsClickLog:DStream[String] = ssc.socketTextStream("spark1",9999)
    val userADsClickLog:DStream[(String,String)] = ADsClickLog.map(tp =>(tp.split(" ")(1),tp))
    val validUserADsClickLog:DStream[String] = userADsClickLog.transform(tp =>{
      val joinRDD: RDD[(String, (String, Option[Boolean]))] = tp.leftOuterJoin(blackListRDD)
      val filterRDD: RDD[(String, (String, Option[Boolean]))] = joinRDD.filter(tp => {
        tp._2._2 match {
          case Some(true) => false
          case _ => true
        }
      })
      val validADsClickLog = filterRDD.map(tp => tp._2._1)
      validADsClickLog
    })
    validUserADsClickLog.print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
