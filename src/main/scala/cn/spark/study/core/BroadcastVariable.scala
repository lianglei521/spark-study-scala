package cn.spark.study.core

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2018/12/29.
  * 共享变量Broadcast
  */
object BroadcastVariable {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("broadcast")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val factor:Int = 2
    val factorBroadcast:Broadcast[Int] = sc.broadcast(factor)
    val numArr:Array[Int] = Array(1,2,3,4,5)
    val numbers:RDD[Int] = sc.parallelize(numArr)
    val multipleNums = numbers.map(num =>{
      num * factorBroadcast.value
    })
    multipleNums.foreach(num => println(num))
    sc.stop()
  }
}
