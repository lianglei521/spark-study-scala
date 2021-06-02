package cn.spark.study.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2019/01/08.
  */
object SaveModeTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("指定保存的模式")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    val peopleDF = sQLContext.read.format("json").load("C:\\Users\\Administrator\\Desktop\\临时文件\\people.json")
    peopleDF.select("name").write.mode(saveMode = "overwrite").format("json").save("C:\\Users\\Administrator\\Desktop\\临时文件\\people2.json")
    sc.stop()
  }
}
