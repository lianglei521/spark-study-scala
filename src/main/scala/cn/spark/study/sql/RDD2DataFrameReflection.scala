package cn.spark.study.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2019/01/07.
  * 如果要用scala开发spark程序
  * 然后在其中，还要实现基于反射的RDD到DataFrame的转换，就必须得用object extends App的方式
  * 不能用def main()方法的方式，来运行程序，否则就会报no typetag for ...class的错误
  */
object RDD2DataFrameReflection extends App{
  //创建SparkContext以及SQLContext对象
  val conf = new SparkConf()
    .setAppName("RDD2DataFrame")
    .setMaster("local")
  val sc = new SparkContext(conf)
  val sQLContext = new SQLContext(sc)

  //导入隐式转化
  import sQLContext.implicits._

  //读取本地文件创建RDD
  val lines:RDD[String] = sc.textFile("C:\\Users\\Administrator\\Desktop\\临时文件\\students.txt",1)

  //创建Student的样例类
  case class Student(id:Int,name:String,age:Int)

  //通过map操作把String类型的RDD转化为Student类型的RDD------这样才能通过toDF转换为DataFrame
  val students:RDD[Student] = lines.map(line =>{
    val lineSplited = line.split(",")
    Student(lineSplited(0).trim.toInt,lineSplited(1).trim,lineSplited(2).trim.toInt)
  })

  //把Student类型的RDD转化为DataFrame-----利用的是导入的sqlContext的隐式转化
  val studentDF:DataFrame = students.toDF()

  //利用studentDF注册临时表，供后面的sql语句查询
  studentDF.registerTempTable("students")

  //sql查询-----结果返回的是DataFrame格式----他的结果可以通过show来查看
  val teenagerDF:DataFrame = sQLContext.sql("select * from students where age <= 18")

  //把dataframe格式的结果通过rdd方法再转化为RDD-----不过类型是Row
  val teenagerRDD:RDD[Row] = teenagerDF.rdd

  //通过map操作把Row转化为Student类型的RDD------Row里面只是包含了数据，没有schema信息
  val teenagerStu:RDD[Student] = teenagerRDD.map(row =>{
    //这个区别于java,顺序是一致的-------重点！！！
    //方式一获取---后面的转换两次
    //Student(row(0).toString.toInt,row(1).toString,row(2).toString.toInt)

    //方式二获取getAs-----以前学的是这种方式
    //Student(row.getAs[Int]("id"),row.getAs[String]("name"),row.getAs[Int]("age"))

    //方式三获取getValuesMap------获取指定的多列-----格式就是键值对形式
    val map = row.getValuesMap(Array("id","name","age"))
    Student(map("id").toString.toInt,map("name").toString,map("age").toString.toInt)
  })

  //将结果收集到Driver端----类似的collect,count,take
  val teenagerArr:Array[Student] = teenagerStu.collect()

  //将结果进行打印
  teenagerArr.foreach(stu => println(stu.id+":"+stu.name+":"+stu.age))

  //释放资源
  sc.stop()

}
