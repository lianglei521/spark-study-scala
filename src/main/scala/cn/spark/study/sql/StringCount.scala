package cn.spark.study.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * author:liangsir 
  * qq:714628767
  * created 2019/01/12.
  */
class StringCount() extends UserDefinedAggregateFunction{

  //inputSchema指的是输入数据的类型
  override def inputSchema: StructType = {
    StructType(Array(StructField("str",StringType,true)))
  }

  //bufferSchema指的是中间进行聚合时，所处理的数据的类型
  override def bufferSchema: StructType = {
    StructType(Array(StructField("count",IntegerType,true)))
  }

  //dataType指的是函数返回值的类型
  override def dataType: DataType = {
    IntegerType
  }


  override def deterministic: Boolean = {
    true
  }

  //为每个分组的数据执行初始化的操作
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }

  //指的是，每个分组，当有新的值进来的时候，如何进行分组对应的聚合值的计算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0) + 1
  }

  //因为spark是分布式的，每个分组的数据，可能分布在多个节点上，要在每个节点上进行局部聚合，就是update
  //但是最后一个分组的聚合值，要在各个节点上的聚合值进行merge,也就是合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
  }

  //一个分组的聚合值，如何通过中间的缓存聚合值，最后返回一个最终的聚合值
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Int](0)
  }
}
