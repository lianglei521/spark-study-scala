package cn.spark.study.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2019/01/08.
  * parquet数据源之使用编程的方式加载数据
  */
object ParquetLoadData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("parquet数据源之使用编程的方式加载数据")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //sqlContext读取本地parquet文件创建DataFrame数据集
    val usersDF = sqlContext.read.parquet("C:\\Users\\Administrator\\Desktop\\临时文件\\users.parquet")
    //DataFrame数据集转化为RDD[Row]---map进行数据的操作----collect收集到driver端----以数组的格式返回
    val nameArr = usersDF.rdd.map(row => "name:"+row.getString(0)).collect()
    //遍历打印
    //方式一
    //nameArr.foreach(name => println(name))
    //方式二
    for(name <- nameArr){
      println(name)
    }
    sc.stop()
  }
}
