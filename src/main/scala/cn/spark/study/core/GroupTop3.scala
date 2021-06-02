package cn.spark.study.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2018/12/30.
  * GroupTop3
  */
object GroupTop3 {
  def main(args: Array[String]): Unit = {
    //创建SparkConf和SparkContext对象
    val conf = new SparkConf()
      .setAppName("GroupTop3")
      .setMaster("local")
    val sc = new SparkContext(conf)
    //读取本地文件创建初始化RDD,后面的参数1指定分区数量
    val lines:RDD[String] = sc. textFile("C:\\Users\\Administrator\\Desktop\\临时文件\\score.txt",1)
    //创建pairRDD
    val pairs:RDD[(String,Int)] = lines.map(line =>{
      val splitedLine:Array[String] = line.split(" ")
      (splitedLine(0),splitedLine(1).toInt)
    })
    //根据班级进行分组
    val groupScore:RDD[(String,Iterable[Int])] = pairs.groupByKey()
    //分别取每组中的前三
    val groupTop3:RDD[(String,Iterable[Int])] = groupScore.map(t =>{
      val className:String = t._1
      //scala中list可以进行排序
      val sortedScore: List[Int] = t._2.toList.sortBy(-_)take(3)
      (className,sortedScore)
    })
    //打印输出
    groupTop3.foreach(t =>{
      println(t._1)
      val scoreTop3:Iterator[Int] = t._2.toIterator
      while (scoreTop3.hasNext){
        println(scoreTop3.next())
      }
    })
    //释放资源
    sc.stop()
  }
}
