package cn.spark.study.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

/**
  * author:liangsir 
  * qq:714628767
  * created 2018/12/28.
  */
object ActionOperation {
  def main(args: Array[String]): Unit = {
     //reduce()
     //collect()
     //count()
     //take()
    countByKey()
  }

  def reduce(): Unit ={
    val conf = new SparkConf()
      .setAppName("reduce")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val numArr = Array(1,2,3,4,5,6,7,8,9)
    val numsRDD:RDD[Int] =sc.parallelize(numArr)
    val sum: Int = numsRDD.reduce(_ + _)//注意RDD是一个弹性分布式数据集
    println(sum)
    sc.stop()
  }
  
  def collect(): Unit ={
    val conf = new SparkConf()
      .setAppName("reduce")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val numArr = Array(1,2,3,4,5,6,7,8,9)
    val numsRDD:RDD[Int] =sc.parallelize(numArr)
    val numbers: Array[Int] = numsRDD.collect()
    for(num <- numbers){
      println(num)
    }
    sc.stop()
  }

  def count(): Unit ={
    val conf = new SparkConf()
      .setAppName("reduce")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val numArr = Array(1,2,3,4,5,6,7,8,9)
    val numsRDD:RDD[Int] =sc.parallelize(numArr)
    val count:Long = numsRDD.count()
    println(count)
    sc.stop()
  }

  def take(): Unit ={
    val conf = new SparkConf()
      .setAppName("reduce")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val numArr = Array(1,2,3,4,5,6,7,8,9)
    val numsRDD:RDD[Int] =sc.parallelize(numArr)
    val top3Num:Array[Int] = numsRDD.take(3)//注意返回值类型
    for(num <- top3Num){
      println(num)
    }
    sc.stop()
  }

  def countByKey(): Unit ={
    val conf = new SparkConf()
      .setAppName("reduce")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val stus = Array(
      Tuple2("class1","leo"),
      Tuple2("class2","tom"),
      Tuple2("class1","lucy"),
      Tuple2("class2","jack"),
      Tuple2("class1","jerry")
    )
    val stuRDD:RDD[(String,String)] =sc.parallelize(stus)
    val stuAndNum: Map[String, Long] = stuRDD.countByKey()
    //println(stuAndNum)
    for((key,value) <- stuAndNum){
      println(key+":"+value)
    }
    sc.stop()
  }

}
