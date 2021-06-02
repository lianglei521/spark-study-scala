package cn.spark.study.core

import org.apache.spark.{ SparkConf, SparkContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2018/12/29.
  */
object AccumulatorVariable extends App {
  val conf = new SparkConf()
    .setAppName("accumulator")
    .setMaster("local")
  val sc = new SparkContext(conf)
  val numArr = Array(1,2,3,4,5)
  val numbers = sc.parallelize(numArr,1)
  val sum= sc.accumulator(0)
  numbers.foreach(num => sum.add(num))
  println(sum.value)
  sc.stop()
}
