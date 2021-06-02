package cn.spark.study.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2018/12/27.
  */
object TransformationOperation {
  def main(args: Array[String]): Unit = {
    //map()
    // filter()
    //flatMap()
    // groupByKey()
    // reduceByKey()
    // sortByKey()
    //join()
    cogroup()
  }
  def map(): Unit ={
    val conf = new SparkConf()
      .setAppName("map")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val nums = Array(1,2,3,4,5)
    val numsRDD: RDD[Int] = sc.parallelize(nums)
    val newNumsRDD: RDD[Int] = numsRDD.map(num => num*2)
    newNumsRDD.foreach(num => println(num))
    sc.stop()
  }

  def filter(): Unit ={
    val conf = new SparkConf()
      .setAppName("filter")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val nums = Array(1,2,3,4,5)
    val numsRDD:RDD[Int] = sc.parallelize(nums,1)
    val evenNumsRDD:RDD[Int] = numsRDD.filter(num => num%2==0)
    evenNumsRDD.foreach(num => println(num))
    sc.stop()
  }

  def flatMap(): Unit ={
    val conf = new SparkConf()
      .setAppName("flatMap")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val str = Array("hello you","hello me","hello you")
    val strRDD:RDD[String] = sc.parallelize(str)
    val wordsRDD:RDD[String] = strRDD.flatMap(str => str.split(" "))
    wordsRDD.foreach(word => println(word))
    sc.stop()
  }

  def groupByKey(): Unit ={
    val conf = new SparkConf()
      .setAppName("groupByKey")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val scoreList = Array(Tuple2("class1",85),Tuple2("class2",86),Tuple2("class3",87),Tuple2("class2",83),("class1",89))
    val scoreRDD:RDD[(String,Int)] = sc.parallelize(scoreList,1)
    val groupScore:RDD[(String,Iterable[Int])] = scoreRDD.groupByKey()
    groupScore.foreach(t =>{
      println(t._1)
      t._2.foreach(singleScore => println(singleScore))
      println("=================")
    })
  }

  def reduceByKey(): Unit ={
    val conf = new SparkConf()
      .setAppName("reduceByKey")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val scores = Array(("leo",10),("lucy",8),("tom",7),("leo",6),("tom",4))
    val scoreRDD:RDD[(String,Int)] = sc.parallelize(scores)
    val reduceScoreRDD:RDD[(String,Int)] = scoreRDD.reduceByKey(_+_)
    reduceScoreRDD.foreach(t => println(t._1+":"+t._2))
    sc.stop()
  }

  def sortByKey(): Unit ={
    val conf = new SparkConf()
      .setAppName("sortByKey")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val scores = Array(("leo",8),("lucy",3),("tom",6),("jack",7))
    val scoresRDD:RDD[(String,Int)] = sc.parallelize(scores)
    val sortRDD:RDD[(String,Int)] = scoresRDD.sortByKey(false)//根据名字首字母的降序排列
    sortRDD.foreach(t => println(t._1+":"+t._2))
    sc.stop()
  }

  def join(): Unit ={
    val conf = new SparkConf()
      .setAppName("join")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val stu = Array((1,"leo"),(2,"lucy"),(3,"tom"))
    val score = Array((1,85),(2,80),(3,75))
    val stuRDD:RDD[(Int,String)] =sc.parallelize(stu)
    val scoRDD:RDD[(Int,Int)] =sc.parallelize(score)
    val stuAndScoRDD: RDD[(Int, (String, Int))] = stuRDD.join(scoRDD)
    stuAndScoRDD.foreach(t =>{
      println(t._1+":"+t._2._1+":"+t._2._2)
    })
    sc.stop()
  }

  def cogroup(): Unit ={
    val conf = new SparkConf()
      .setAppName("cogroup")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val students = Array((1,"leo"),(2,"lucy"),(2,"tom"))
    val scores  = Array((1,85),(2,86),(2,87))
    val stuRDD:RDD[(Int,String)] = sc.parallelize(students)
    val scoRDD:RDD[(Int,Int)] = sc.parallelize(scores)
    val cogroupRDD:RDD[(Int,(Iterable[String],Iterable[Int]))] = stuRDD.cogroup(scoRDD)
    cogroupRDD.foreach(t =>{
      println(t._1+":"+t._2._1.toList+":"+t._2._2.toList)
    })
    sc.stop()
  }
}
