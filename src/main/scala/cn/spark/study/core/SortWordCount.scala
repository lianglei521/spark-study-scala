package cn.spark.study.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2018/12/29.
  */
object SortWordCount {

  def main(args: Array[String]): Unit = {
    //创建SparkConf对象
    val conf = new SparkConf()
      .setAppName("sortWordCount")
      .setMaster("local")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    //读取本地文件创建初始化RDD
    val lines:RDD[String] = sc.textFile("C:\\Users\\Administrator\\Desktop\\临时文件\\hello.txt")
    //创建words RDD
    val words:RDD[String] = lines.flatMap(line => line.split(" "))
    //创建wordAndOne RDD
    val wordAndOne:RDD[(String,Int)] = words.map(word => (word,1))
    //创建wordCount RDD
    val wordCount:RDD[(String,Int)] = wordAndOne.reduceByKey(_+_)
    //反转wordCount RDD------> countWord RDD
    val countWord:RDD[(Int,String)] = wordCount.map(t => (t._2,t._1))
    //基于countWord RDD的key进行降序排序
    val sortCountWord:RDD[(Int,String)] = countWord.sortByKey(false)
    //反转sortCountWord RDD---->sortWordCount RDD
    val sortWordCount:RDD[(String,Int)] = sortCountWord.map(t => (t._2,t._1))
    //打印输出
    sortWordCount.foreach(t => println(t._1+":"+t._2))
    //释放资源
    sc.stop()
  }
}
