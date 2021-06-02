package cn.spark.study.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2019/01/06.
  */
object DataFrameOperation {
  def main(args: Array[String]): Unit = {
    //创建DataFrame
    val conf = new SparkConf()
      .setAppName("DataFrameOperation")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json("C:\\Users\\Administrator\\Desktop\\临时文件\\students.json")
    //显示全部数据
    df.show()
    //显示schema信息
    df.printSchema()
    //显示某一列的几行数据
    df.select("name").show(1)
    //显示某几列，并对列进行计算
    df.select(df.col("name"),df.col("age").plus(1)).show()
    //根据某一列的值进行过滤
    df.filter(df.col("age").lt(18)).show()
    //按age分组聚合
    df.groupBy(df.col("age")).count().show()

    //释放资源
    sc.stop()
  }
}
