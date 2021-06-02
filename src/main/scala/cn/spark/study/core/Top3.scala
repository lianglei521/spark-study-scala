package cn.spark.study.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2018/12/30.
  * 取最大的前三个值
  */
object Top3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Top3")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val lines:RDD[String] = sc.textFile("C:\\Users\\Administrator\\Desktop\\临时文件\\top.txt",1)
    val pairs:RDD[(Int,String)] = lines.map(line =>(line.toInt,line))
    val sortedPair:RDD[(Int,String)] = pairs.sortByKey(false)
    val sortedLine:RDD[Int] = sortedPair.map(t => t._1)
    val top3:Array[Int] = sortedLine.take(3)
    for(num <- top3){
      println(num)
    }
    sc.stop()
  }
}
