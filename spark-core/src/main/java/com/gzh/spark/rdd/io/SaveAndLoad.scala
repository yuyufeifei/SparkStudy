package com.gzh.spark.rdd.io

import org.apache.spark.{SparkConf, SparkContext}

object SaveAndLoad {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("a", 2)))
    rdd.saveAsTextFile("output")
    rdd.saveAsObjectFile("output1")
    rdd1.saveAsSequenceFile("output2")  //数据需为K-V类型

    sc.objectFile[Int]("output1").collect().foreach(println)
    sc.sequenceFile[String, Int]("output2").collect().foreach(println)

  }
}
