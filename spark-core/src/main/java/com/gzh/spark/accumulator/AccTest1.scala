package com.gzh.spark.accumulator

import org.apache.spark.{SparkConf, SparkContext}

object AccTest1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1,2,3,4,5))

    // 此种方式打印sum还是0
    // var sum = 0

    // 声明累加器
    // 少加：转换算子（如map）中调用累加器，如果没有行动算子（如foreach），则不会执行
    // 多加：调用了多次行动算子（如collect）
    // 一般情况下，累加器会放置在行动算子中进行操作
    var sumAcc = sc.longAccumulator("sum");
//    sc.doubleAccumulator("sum")
//    sc.collectionAccumulator("sum")

    rdd.foreach(
      num => {
        // 使用累加器
        sumAcc.add(num)
      }
    )
    // 获取累加器的值
    println("sum = " + sumAcc.value)

    sc.stop()
  }
}
