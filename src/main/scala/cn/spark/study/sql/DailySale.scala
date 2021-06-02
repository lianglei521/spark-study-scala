package cn.spark.study.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

/**
  * author:liangsir 
  * qq:714628767
  * created 2019/01/12.
  * 统计网站每日用户销售总额
  */
object DailySale {
  def main(args: Array[String]): Unit = {
    //屏蔽日子
    Logger.getLogger("org").setLevel(Level.WARN)
    //构建SparkConf,SparkContext,SQLContext对象
    val conf = new SparkConf()
      .setAppName("统计网站每日用户销售总额")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //导入sqlContext的隐式转化---可以让DataFrame数据集操作SparkSQL的内置函数
    import sqlContext.implicits._

    // 说明一下，业务的特点
    // 实际上呢，我们可以做一个，单独统计网站登录用户的销售额的统计
    // 有些时候，会出现日志的上报的错误和异常，比如日志里丢了用户的信息，那么这种，我们就一律不统计了
    //构建用户访问日志信息
    val userLogArr = Array("2015-10-01,55.05,1122",
      "2015-10-01,23.15,1133",
      "2015-10-01,15.20,",
      "2015-10-02,56.05,1144",
      "2015-10-02,78.87,1155",
      "2015-10-02,113.02,1123")

    //并行化集合，创建初始化RDD------设置并行度参数为5------通常为cpu核数的2~3倍
    val userLogRDD = sc.parallelize(userLogArr,5)

    //把一些不完整的日志数据过滤掉
    val filteredUserLogRDD = userLogRDD.filter(line => if(line.split(",").length >= 3) true else false)

    //把RDD[String]转化为-->RDD[Row]
    val filteredUserLogRowRDD = filteredUserLogRDD.map(line =>Row(line.split(",")(0),line.split(",")(1).toDouble))

    //构建schema信息---也就是StructType
    val structType = StructType(Array(
      StructField("date",StringType,true),
      StructField("sale",DoubleType,true)
    ))

    //由RDD[Row]+StructType生成DataFrame数据集
    val userLogDateAndSaleDF = sqlContext.createDataFrame(filteredUserLogRowRDD,structType)

    //使用SparkSQL的内置函数进行分组聚合统计操作----必须导入以下的这个包才可使用
   //import org.apache.spark.sql.functions._
    userLogDateAndSaleDF.groupBy("date")
      .agg('date,sum('sale))
      .map(row => Row(row(1),row(2)))
      .collect()
      .foreach(row => println(row))



    //释放资源
    sc.stop()

  }
}
