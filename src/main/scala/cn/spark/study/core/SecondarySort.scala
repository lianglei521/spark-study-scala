package cn.spark.study.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2018/12/29.
  */
object SecondarySort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("基于自定义key二次排序")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val lines:RDD[String] = sc.textFile("C:\\Users\\Administrator\\Desktop\\临时文件\\secondarySort.txt")
    val pairs:RDD[(SecondarySortKey,String)] = lines.map(line =>{
      (new SecondarySortKey(line.split(" ")(0).toInt,line.split(" ")(1).toInt),line)
    })
    val sortedPairs:RDD[(SecondarySortKey,String)] = pairs.sortByKey(false)
    val sortedLines = sortedPairs.map(t => t._2)
    sortedLines.foreach(line => println(line))
    sc.stop()
  }

}
