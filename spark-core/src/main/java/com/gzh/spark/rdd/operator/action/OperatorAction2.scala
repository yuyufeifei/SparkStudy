package com.gzh.spark.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object OperatorAction2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    // 行动算子

    // aggregate  分区的数据通过初始值和分区内的数据进行聚合，然后再和初始值进行分区间的数据聚合
    // aggregateByKey：初始值只参与分区内计算
    // aggregate：初始值参与分区内和分区间计算
    println(rdd.aggregate(10)(_ + _, _ + _))  //结果为40

    // fold   aggregate的简化版
    println(rdd.fold(10)(_ + _))

    // countByValue 统计值出现的次数
    println(rdd.countByValue())
    /* 结果
    Map(4 -> 1, 2 -> 1, 1 -> 1, 3 -> 1)
    */

    val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("a", 2)))

    // countByKey   统计key出现的次数
    println(rdd1.countByKey())
    /* 结果
    Map(a -> 2, b -> 1, c -> 1)
    */

    // save
    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")
    rdd1.saveAsSequenceFile("output2")  //数据需为K-V类型

    // foreach
    // Driver端内存集合的循环遍历方法，结果有序
    rdd.collect().foreach(println)
    // Executor端内存数据打印，结果无序
    rdd.foreach(println)


    sc.stop()
  }
}
