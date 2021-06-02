package cn.spark.study.sql

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2019/01/08.
  * 多个Parquet文件合并schema信息
  */
object ParquetMergeSchema {
  //创建SaprkConf,SparkContext,SQLContext对象
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("多个Parquet文件合并schema信息")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    //导入隐式转换
    import sQLContext.implicits._

    //创建第一个DataFrame数据集，并保存为parquet格式的文件-----toSeq也可以不写，因为Seq(它是trait)的子类里包括String,List,Array
    val studentsWithNameAge = Array(("leo",18),("lucy",19)).toSeq
    val studentsWithNameAgeDF = sc.parallelize(studentsWithNameAge,2).toDF("name","age")
    studentsWithNameAgeDF.save("C:\\Users\\Administrator\\Desktop\\临时文件\\students","parquet",SaveMode.Append)

    //创建第二个DataFrame数据集，并保存为parquet格式的文件
    val studentsWithNameGrade = Array(("tom","A"),("jerry","B")).toSeq
    val studentsWithNameGradeDF = sc.parallelize(studentsWithNameGrade,2).toDF("name","grade")
    studentsWithNameGradeDF.save("C:\\Users\\Administrator\\Desktop\\临时文件\\students","parquet",SaveMode.Append)

    //首先两个DataFrame的元数据不一样吧，一个是name,age,一个是name,grade
    //我们期望的是读取出来的数据，自动合并两个文件的元数据，出现三个列name,age,grade
    //那么就是mergeSchema的方式，读取students中的数据，进行元数据的合并
    //1、读取Parquet文件时，将数据源的选项，mergeSchema，设置为true
    //2、使用SQLContext.setConf()方法，将spark.sql.parquet.mergeSchema参数设置为true
    val students = sQLContext.read.option("mergeSchema","true").parquet("C:\\Users\\Administrator\\Desktop\\临时文件\\students")
    students.printSchema()
    students.show()
    sc.stop()
  }
}
