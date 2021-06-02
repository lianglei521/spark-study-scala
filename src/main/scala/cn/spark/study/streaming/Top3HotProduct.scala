package cn.spark.study.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2019/01/23.
  */
object Top3HotProduct {
  //屏蔽掉INFO信息
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Top3HotProduct")
      .setMaster("local[2]")
    val ssc = new StreamingContext(conf,Seconds(1))
    //日志格式 leo a phone
    val lines:ReceiverInputDStream[String] = ssc.socketTextStream("spark1",9999)
    val pairs:DStream[(String,Int)] = lines.map(line =>(line.split(" ")(2)+"_"+line.split(" ")(1),1))
    //滑动窗口函数聚合操作
    val categoryProductCountsDS:DStream[(String,Int)] = pairs.reduceByKeyAndWindow(
      (v1:Int,v2:Int) => v1 + v2 ,//第一个参数是一个匿名函数，实现聚合操作
      Seconds(60),//这个参数是窗口函数的大小，60秒一个窗口函数
      Seconds(10)//这个参数是每隔10秒，从最近的时间开始建一个窗口
    )
    //整合SparkSQL,继续处理数据集
    categoryProductCountsDS.foreachRDD(categoryProductCountsRDD =>{
      //转化为RowRDD
      val categoryProductCountsRowRDD = categoryProductCountsRDD.map(tp => Row(tp._1.split("_")(0),tp._1.split("_")(1),tp._2))
      //构建schema信息
      val structType:StructType = StructType(Array(
        StructField("category",StringType,true),
        StructField("product",StringType,true),
        StructField("count",IntegerType,true)
      ))
      //通过RDD[Row]+Schema创建DateFrame
      val hiveContext = new HiveContext(categoryProductCountsRDD.context)
      val categoryProductCountsDF: DataFrame = hiveContext.createDataFrame(categoryProductCountsRowRDD,structType)
      //注册临时表
      categoryProductCountsDF.registerTempTable("categoryProductCounts")
      val sql =
        """
          |select category,product,count
          |from (
          |   select
          |     category,
          |     product,
          |     count,
          |     row_number() over(partition by category order by count desc) rank
          |    from
          |     categoryProductCounts
          | )temp
          | where rank <=3
        """.stripMargin
      val top3Product: DataFrame = hiveContext.sql(sql)
      top3Product.show()
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
