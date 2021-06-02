package cn.spark.study.sql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2019/01/12.
  * UDF 针对的是单行输入，返回一个输出
  */
object UDF {
  //屏蔽日志
  Logger.getLogger("org").setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    //创建SparkConf,SparkContext,SQLContext对象
    val conf = new SparkConf()
      .setAppName("UDF")
      .setMaster("local[1]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //构建数据集
    val nameArr = Array("Leo","Marry","Jack","Tom")
    val nameRDD = sc.parallelize(nameArr,5)

    //把RDD转换为RDD[Row]
    val nameRowRDD = nameRDD.map(name => Row(name))

    //构建schema信息
    val structType = StructType(Array(StructField("name",StringType,true)))

    //创建DataFrame数据集
    val nameDF = sqlContext.createDataFrame(nameRowRDD,structType)

    //注册临时表
    nameDF.registerTempTable("names")

    //UDF自定义函数(UDF：User Defined Function。用户自定义函数)
    //方法为sqlContext.udf.register()
    //里面两个参数，第一个参数是函数的注册名字，第二个参数是定义的匿名函数
    sqlContext.udf.register("strLen",(name:String)=>name.length)

    //开始使用自定义函数
    val nameAngLenDF: DataFrame = sqlContext.sql("select name, strLen(name) from names")

    //收集并打印
    nameAngLenDF.collect().foreach(println)

    //释放资源
    sc.stop()

  }
}
