package com.gzh.spark.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark中的广播变量可以将闭包的数据保存到executor的内存中
 * spark的广播变量不能更改
 */
object BcTest1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("AccWordCount")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List( ("a",1), ("b", 2), ("c", 3), ("d", 4) ),4)
    val list = List( ("a",4), ("b", 5), ("c", 6), ("d", 7) )
    // 声明广播变量
    val broadcast: Broadcast[List[(String, Int)]] = sc.broadcast(list)
    val resultRDD: RDD[(String, (Int, Int))] = rdd1.map {
      case (key, num) =>
        var num2 = 0
        // 使用广播变量
        for ((k, v) <- broadcast.value) {
          if (k == key) {
            num2 = v
          }
        }
        (key, (num, num2))
    }


    sc.stop()
  }
}
