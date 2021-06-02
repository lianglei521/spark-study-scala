package cn.spark.study.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2019/01/07.
  */
object RDD2DataFrameProgrammatically extends App{
  //创建SparkConf,Sparkcontext,SQLContext对象
  val conf = new SparkConf()
    .setAppName("")
    .setMaster("local")
  val sc = new SparkContext(conf)
  val sQLContext = new SQLContext(sc)

  //读取本地文件创建初始化RDD
  val lines:RDD[String] = sc.textFile("C:\\Users\\Administrator\\Desktop\\临时文件\\students.txt")

  //构建RowRDD----里面封装了具体的数据
  val students:RDD[Row] = lines.map(line =>{
    Row(line.split(",")(0).trim.toInt,
        line.split(",")(1).trim,
        line.split(",")(2).trim.toInt
    )
  })

  //动态构建schema信息---也就是元数据
  val structType = new StructType(Array(
    StructField("id",IntegerType,true),
    StructField("name",StringType,true),
    StructField("age",IntegerType,true)
  ))

  //RDD------>DataFrame
  val studentDF =sQLContext.createDataFrame(students,structType)

  //注册临时表
  studentDF.registerTempTable("students")

  //sql查询
  val teenagerDF = sQLContext.sql("select * from students where age <= 18")

  //DataFrame-------->RDD[Row]------>Array[Row]----->打印
  teenagerDF.rdd.collect().foreach(row => println(row))

  //释放资源
  sc.stop()

}
