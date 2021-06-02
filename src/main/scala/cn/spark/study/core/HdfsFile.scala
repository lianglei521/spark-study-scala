package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2018/12/25.
  * 本地文件创建RDD
  * 统计文本文件中的字数
  */
object HdfsFile extends App{

  val conf = new SparkConf()
    .setAppName("统计文本文件中的字数")
  val sc = new SparkContext(conf)
  val lines = sc.textFile("hdfs://spark1:9000/新建日记本文档.jnt")
  val lineLength = lines.map(line => line.length)
  val count = lineLength.reduce(_+_)
  println("文本文件字数为："+count)
  sc.stop()

}
