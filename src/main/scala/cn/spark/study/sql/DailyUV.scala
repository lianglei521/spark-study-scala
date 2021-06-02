package cn.spark.study.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, GroupedData, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2019/01/11.
  * 日常用户UV访问统计
  */
object DailyUV {
  def main(args: Array[String]): Unit = {
    //设置控制台的日志打印级别----屏蔽掉INFO信息
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf()
      .setAppName("日常用户UV访问统计")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    //sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)
    // 这里着重说明一下！！！
    // 要使用Spark SQL的内置函数，就必须在这里导入SQLContext下的隐式转换
    import sqlContext.implicits._

    //构建用户访问日志数据,并创建RDD
    // 模拟用户访问日志，日志用逗号隔开，第一列是日期，第二列是用户id
    val userAccessLog = Array(
      "2015-10-01,1122",
      "2015-10-01,1122",
      "2015-10-01,1123",
      "2015-10-01,1124",
      "2015-10-01,1124",
      "2015-10-02,1122",
      "2015-10-02,1121",
      "2015-10-02,1123",
      "2015-10-02,1123");
    //创建元素为lines的RDD
    val userAccessLogLineRDD = sc.parallelize(userAccessLog,5)
    //创建数据为row的RDD------注意转化为int
    val userAccessLogRowRDD: RDD[Row] = userAccessLogLineRDD.map(log =>Row(log.split(",")(0),log.split(",")(1).toInt))
    //创建StructType也就是schema信息
    val structType = new StructType(Array(
      StructField("date",StringType,true),
      StructField("userid",IntegerType,true)
    ))
    //创建DataFrame数据集
    val userAccessLogRowDF = sqlContext.createDataFrame(userAccessLogRowRDD,structType)
    //userAccessLogRowDF.printSchema()
   // userAccessLogRowDF.show()

    //接下来就可以使用内置函数进行操作
    // 这里讲解一下uv的基本含义和业务
    // 每天都有很多用户来访问，但是每个用户可能每天都会访问很多次
    // 所以，uv，指的是，对用户进行去重以后的访问总数

    // 这里，正式开始使用Spark 1.5.x版本提供的最新特性，内置函数，countDistinct
    // 讲解一下聚合函数的用法
    // 首先，对DataFrame调用groupBy()方法，对某一列进行分组
    // 然后，调用agg()方法 ，第一个参数，必须，必须，传入之前在groupBy()方法中出现的字段
    // 第二个参数，传入countDistinct、sum、first等，Spark提供的内置函数
    // 内置函数中，传入的参数，也是用单引号作为前缀的，其他的字段
    val userAccessLogGroupByDate: GroupedData = userAccessLogRowDF.groupBy("date")
    val userAccessLogGroupByDate2CountByUserid: DataFrame = userAccessLogGroupByDate.agg('date, countDistinct('userid))
    userAccessLogGroupByDate2CountByUserid.show()
    userAccessLogGroupByDate2CountByUserid.printSchema()
    val userAccessLogGroupByDate2CountByUseridRowRDD: RDD[Row] = userAccessLogGroupByDate2CountByUserid.map(row => Row(row(1),row(2)))
    val userAccessLogRowArr: Array[Row] = userAccessLogGroupByDate2CountByUseridRowRDD.collect()
    userAccessLogRowArr.foreach(row => println(row))

    sc.stop()
  }
}
