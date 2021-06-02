package cn.spark.study.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * author:liangsir 
  * qq:714628767
  * created 2018/12/25.
  * 并行集合创建RDD
  * 求1~10的和
  */
object ParallelizeCollection extends App{

  //创建sparkConf对象
  val conf = new SparkConf()
    .setAppName("求1~10的和")
      .setMaster("local")

  //创建SparkContext对象
  val sc = new SparkContext(conf)

  //创建集合对象
  val nums = Array(1,2,3,4,5,6,7,8,9,10)

  //并行集合创建RDD
  val numsRDD = sc.parallelize(nums,3)

  //求和
  val sum = numsRDD.reduce(_+_)

  //输出结果
  println("1到10的和为："+sum)

  //释放资源
  sc.stop()

}
