package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2018/12/26.
  * 统计每行出现的次数
  */
object LineCount {
  def main(args: Array[String]): Unit = {
    //创建SparkConf对象
    val conf = new SparkConf()
      .setAppName("统计每行出现的次数")
      .setMaster("local")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    //读取本地文件创建RDD
    val lines = sc.textFile("C:\\Users\\Administrator\\Desktop\\临时文件\\hello.txt")
    //创建pairRDD
    val pair = lines.map(line => (line,1))
    //创建lineCountRDD
    val lineCounts = pair.reduceByKey(_+_)
    //遍历打印
    lineCounts.foreach(lineCount => println(lineCount._1+"出现了："+lineCount._2+"次"))
    //释放资源
    sc.stop()
  }

}
