package com.gzh.spark.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *  从集合（内存）中创建 RDD
 */
object Memory_partition {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // makeRDD的第二个参数表示分区的数量，默认值与运行环境相关
    val rdd: RDD[Int] = sc.makeRDD(
      List(1, 2, 3, 4), 3
    )
    rdd.saveAsTextFile("output")

    sc.stop()
  }
}
