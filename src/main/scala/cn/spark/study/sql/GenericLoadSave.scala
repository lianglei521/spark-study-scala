package cn.spark.study.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2019/01/08.
  * 通用的load和save操作--------操作的是parquet文件
  */
object GenericLoadSave {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("GenericLoadSave")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val usersDF = sqlContext.read.load("C:\\Users\\Administrator\\Desktop\\临时文件\\users.parquet")
    usersDF.select("name","favorite_color").write.save("C:\\Users\\Administrator\\Desktop\\临时文件\\users2.parquet")
    sc.stop()
  }
}
