package com.gzh.spark.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SerializableTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("serial")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

    val search = new Search("hello")
    // 函数传递，打印：ERROR Task not serializable
    search.getMatch1(rdd).collect().foreach(println)
    // 属性传递，打印：ERROR Task not serializable
    search.getMatch2(rdd).collect().foreach(println)

    sc.stop()
  }
}

// 两种方式皆可
// class Search(query:String) extends Serializable
// case class Search(query:String)
class Search(query:String) extends Serializable {
  def isMatch(s: String): Boolean = {
    s.contains(query)
  }
  // 函数序列化案例
  def getMatch1 (rdd: RDD[String]): RDD[String] = {
    //rdd.filter(this.isMatch)
    rdd.filter(isMatch)
  }
  // 属性序列化案例
  def getMatch2(rdd: RDD[String]): RDD[String] = {
    //rdd.filter(x => x.contains(this.query))
    rdd.filter(x => x.contains(query))

    // 此种方式不需序列化
    //val q = query
    //rdd.filter(x => x.contains(q))
  }
}
