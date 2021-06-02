package cn.spark.study.sql

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * author:liangsir 
  * qq:714628767
  * created 2019/01/09.
  * JSON数据源
  */
object JSONDataSource {
  def main(args: Array[String]): Unit = {
    //创建SaprkConf,SparkContext,SQLContext对象
    val conf = new SparkConf()
      .setAppName("JSONDataSource")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //sqlContext通过read方法+json方法读取本地json文件，创建DataFrame数据集
    val studentNameAndScoreDF = sqlContext.read.json("C:\\Users\\Administrator\\Desktop\\临时文件\\students.json")
    //注册临时表，并执行sql查询成绩大于80分的学生
    studentNameAndScoreDF.registerTempTable("studentNameAndScore")
    val goodStudentNameAndScoreDF = sqlContext.sql("select name,score from studentNameAndScore where score >=80")
    //把好学生的名字收集到driver端
    val goodStudentNamesArr:Array[String] = goodStudentNameAndScoreDF.rdd.map(row => row.getAs[String]("name")).collect()
    //把这个集合用广播出去
    val goodStudentNamesbroadcast: Broadcast[Array[String]] = sc.broadcast(goodStudentNamesArr)

    //创建学生基本信息的DataFrame数据集
    val studentNameAndAgeJSONsArr = new ArrayBuffer[String]()
    studentNameAndAgeJSONsArr.append("{\"name\":\"Leo\",\"age\":18}")
    studentNameAndAgeJSONsArr.append("{\"name\":\"Marry\",\"age\":19}")
    studentNameAndAgeJSONsArr.append("{\"name\":\"Jack\",\"age\":17}")
    val studentNameAndAgeJSONsRDD = sc.parallelize(studentNameAndAgeJSONsArr,1)
    val studentNameAndAgeJSONsDF = sqlContext.read.json(studentNameAndAgeJSONsRDD)
    //测试代码
    //studentNameAndAgeJSONsDF.show()
    //注册临时表--执行sql语查询好学生的基本信息
    studentNameAndAgeJSONsDF.registerTempTable("studentNameAndAge")
    var sql = "select name,age from studentNameAndAge where name in ("
    val goodStudentNames:Array[String] = goodStudentNamesbroadcast.value
    for(i <- 0 until(goodStudentNames.length)){
      //这一步忘记单引号出现的错误很难发现！！！
      sql += "'"+goodStudentNames(i)+"'"
      if(i<goodStudentNames.length-1){
        sql += ","
      }
    }
    sql += ")"
    val goodStudentNameAndAgeDF = sqlContext.sql(sql)

    //goodStudentNameAndAgeDF.show()
   // goodStudentNameAndAgeDF.printSchema()

    //join操作把好学生的成绩和年龄整合到一起
    val goodStudent:RDD[(String,(Long,Long))] = goodStudentNameAndScoreDF.rdd.map(row =>(row.getAs[String]("name"),row.getAs[Long]("score")))
      .join (
        goodStudentNameAndAgeDF.rdd.map(row =>(row.getAs[String]("name"),row.getAs[Long]("age")))
      );

    //测试
    //goodStudent.foreach(t => println(t._1+":"+t._2._1+":"+t._2._2))

    //把RDD[(String,(Long,Long))]---->RDD[Row]
    val goodStudentNameAndAgeAndScore:RDD[Row] = goodStudent.map(t =>Row(t._1,t._2._2,t._2._1))

    //构建StructType信息
    val structType = new StructType(Array(
      StructField("name",StringType,true),
      StructField("age",LongType,true),
      StructField("score",LongType,true)
    ))

    //通过RDD[Row]+StructType创建DataFrame数据集
    val goodStudentNameAndAgeAndScoreDF = sqlContext.createDataFrame(goodStudentNameAndAgeAndScore,structType)
    //保存为json文件
    goodStudentNameAndAgeAndScoreDF.registerTempTable("goodStudent")
    sqlContext.sql("select name,age,score from goodStudent").write.format("json").save("C:\\Users\\Administrator\\Desktop\\临时文件\\good_student.json")

    //释放资源
    sc.stop()
  }
}
