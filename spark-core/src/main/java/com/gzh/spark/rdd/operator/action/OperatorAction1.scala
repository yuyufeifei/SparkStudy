package com.gzh.spark.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object OperatorAction1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    // 行动算子 触发作业（Job）执行的方法。底层代码调用的时环境对象的runJob方法，创建ActiveJob，提交执行

    // reduce   先聚合分区内数据，再聚合分区间数据
    println(rdd.reduce(_ + _))

    // collect  将不同分区的数据按照分区顺序采集到Driver端内存，形成数组
    println(rdd.collect().mkString(","))

    // count    获取个数
    println(rdd.count())

    // first    获取第一个数据
    println(rdd.first())

    // take     获取前N个数据
    println(rdd.take(2).mkString(","))

    val rdd1 = sc.makeRDD(List(2, 3, 1, 4))

    // takeOrdered  数据排序后，获取前N个。想要降序，添加(Ordering.Int.reverse)
    println(rdd1.takeOrdered(2).mkString(","))
    println(rdd1.takeOrdered(2)(Ordering.Int.reverse).mkString(","))

    sc.stop()
  }
}
