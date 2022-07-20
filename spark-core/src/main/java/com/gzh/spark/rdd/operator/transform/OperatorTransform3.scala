package com.gzh.spark.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object OperatorTransform3 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2 = sc.makeRDD(List(3, 4, 5, 6))

    // 算子 - 双Value类型

    // 交、并、差集要求两个数据源数据类型一致
    // 交集   3,4
    println(
      rdd1.intersection(rdd2)
        .collect().mkString(",")
    )

    // 并集   1,2,3,4,3,4,5,6
    println(
      rdd1.union(rdd2)
        .collect().mkString(",")
    )

    // 差集   1,2
    println(
      rdd1.subtract(rdd2)
        .collect().mkString(",")
    )

    // 拉链   (1,3),(2,4),(3,5),(4,6)
    // 数据源数据类型可以不一致；但要求两个数据源的分区数和每个分区里的元素个数一致
    println(
      rdd1.zip(rdd2)
        .collect().mkString(",")
    )

    sc.stop()
  }
}
