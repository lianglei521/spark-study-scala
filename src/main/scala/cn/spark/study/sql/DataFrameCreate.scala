package cn.spark.study.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2019/01/06.
  */
object DataFrameCreate {
  def main(args: Array[String]): Unit = {
    //创建SparkConf对象
    val conf = new SparkConf()
      .setAppName("DataFrameCreate")
      .setMaster("local")
    //创建SparkContext的对象
    val sc = new SparkContext(conf)
    //创建SQLContext对象
    val sqlContext = new SQLContext(sc)
    //读取本地json文件创建DataFrame对象
    val df = sqlContext.read.json("C:\\Users\\Administrator\\Desktop\\临时文件\\students.json")
    //展示全部数据
    df.show()
    //释放资源
    sc.stop()
  }
}
