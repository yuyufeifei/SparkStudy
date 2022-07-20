package com.gzh.spark.rdd.dependence

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * toDebugString 显示血缘关系
 * dependencies  显示依赖关系
 */
object DependenceTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val fileRDD: RDD[String] = sc.textFile("data/1.txt")
    println("--------Debug--------")
    println(fileRDD.toDebugString)
    println("--------dependencies--------")
    println(fileRDD.dependencies)
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    println("--------Debug--------")
    println(wordRDD.toDebugString)
    println("--------dependencies--------")
    println(wordRDD.dependencies)
    val mapRDD: RDD[(String, Int)] = wordRDD.map((_,1))
    println("--------Debug--------")
    println(mapRDD.toDebugString)
    println("--------dependencies--------")
    println(mapRDD.dependencies)
    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    println("--------Debug--------")
    println(resultRDD.toDebugString)
    println("--------dependencies--------")
    println(resultRDD.dependencies)

    resultRDD.collect()

    sc.stop()
  }
}
