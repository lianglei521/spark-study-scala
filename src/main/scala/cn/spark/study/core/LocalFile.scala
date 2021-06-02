package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2018/12/25.
  * 本地文件创建RDD
  * 统计文本文件中的字数
  */
object LocalFile extends App{

  val conf = new SparkConf()
    .setAppName("统计文本文件中的字数")
      .setMaster("local")
  val sc = new SparkContext(conf)
  val lines = sc.textFile("C:\\Users\\Administrator\\Desktop\\临时文件\\新建日记本文档.jnt")
  val lineLength = lines.map(line => line.length)
  val count = lineLength.reduce(_+_)
  println("1到10的和为："+count)
  sc.stop()

}
