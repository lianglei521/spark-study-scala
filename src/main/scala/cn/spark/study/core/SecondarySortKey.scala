package cn.spark.study.core

/**
  * author:liangsir 
  * qq:714628767
  * created 2018/12/29.
  * 自定义key
  */
class SecondarySortKey(val first:Int,val second:Int) extends Ordered[SecondarySortKey] with Serializable {
  override def compare(that: SecondarySortKey): Int = {
    if(this.first - that.first != 0){
      return this.first - that.first
    }else{
      return this.second - that.second
    }
  }
}
