package cn.spark.study.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2019/01/11.
  */
object HiveDataSource {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("HiveDataSource")
    val sc = new SparkContext(conf)
    val hiveContext =new HiveContext(sc)

    hiveContext.sql("drop table if exists student_infos")
    hiveContext.sql("create table if not exists student_infos (name string,age int)")
    hiveContext.sql("load data local inpath '/usr/local/spark-study/resources/student_infos.txt' into table student_infos")

    hiveContext.sql("drop table if exists student_scores")
    hiveContext.sql("create table if not exists student_scores (name string,score int)")
    hiveContext.sql("load data local inpath '/usr/local/spark-study/resources/student_scores.txt' into table student_scores")

    val goodStudentDF = hiveContext.sql("select si.name ,si.age ,ss.score  "
      +"from student_infos si "
      +"join student_scores ss on si.name = ss.name "
      +"where ss.score >= 80"
    )

    hiveContext.sql("drop table if exists good_student_infos")
    goodStudentDF.saveAsTable("good_student_infos")

    val good_student_row: Array[Row] = goodStudentDF.collect()
    for (good_student <- good_student_row){
      println(good_student)
    }

    sc.stop()
  }
}
