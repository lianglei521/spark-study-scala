package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2018/12/24.
  */
object WordCountScala {
  def main(args: Array[String]): Unit = {

  val conf = new SparkConf()
  conf.setAppName("WordCountScala")
  val sc = new SparkContext(conf)
  val lines = sc.textFile("hdfs://spark1:9000/spark.txt")
  //val lines = sc.textFile("./src/spark.txt",1)
  val words = lines.flatMap{line => line.split(" ")}
  val wordAndOne = words.map{word => (word ,1)}
  val wordCount = wordAndOne.reduceByKey{_+_}
  wordCount.foreach(wordCount => println(wordCount._1+"出现了"+wordCount._2+"次"))

    sc.stop()
  }




}
