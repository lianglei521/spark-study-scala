package cn.spark.study.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2019/01/08.
  * 手动指定数据源--------这种方式牛逼！！！！
  */
object ManuallySpecifyOptions {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("手动指定数据源")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //通过format手动指定读取和存储的数据源
    val peopleDF = sqlContext.read.format("json").load("C:\\Users\\Administrator\\Desktop\\临时文件\\people.json")
    peopleDF.select("name","age").write.format("json").save("C:\\Users\\Administrator\\Desktop\\临时文件\\people2.json")

    sc.stop()

  }
}
