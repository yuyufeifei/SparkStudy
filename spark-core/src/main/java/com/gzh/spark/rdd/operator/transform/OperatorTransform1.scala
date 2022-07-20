package com.gzh.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object OperatorTransform1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // 算子 - map
    // RDD计算一个分区内的数据是一个一个执行逻辑，只有前面一个数据全部的逻辑执行完毕后，才会执行下一个数据。
    // 分区内数据的执行是有序的，不同分区内的数据执行是无序的
    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    val mapRDD = rdd.map(_ * 2)
    mapRDD.collect().foreach(println)

    val rdd1 = sc.textFile("data/1.txt")
    val mapRDD1 = rdd1.map(
      line => {
        line.split(" ")(1)
      }
    )
    mapRDD1.collect().foreach(println)

    // 算子 - mapPartitions
    // 可以以分区为单位进行数据转换操作，但是会将整个分区的数据加载到内存进行引用，如果处理完的数据是不会被释放掉，存在对象的引用。
    // 在内存较小、数据量较大的场合下，容易出现内存溢出。
    val mapPartitionsRDD = sc.makeRDD(List(1, 2, 3, 4), 2).mapPartitions(_.map( _ * 2 ))
    mapPartitionsRDD.collect().foreach(println)

    // 得到分区内的最大值
    sc.makeRDD(List(1, 2, 3, 4), 2).mapPartitions(
      iter => {
        List(iter.max).iterator
      }
    ).collect().foreach(println)

    // 算子 - mapPartitionsWithIndex
    // 保留第二个分区的数据
    sc.makeRDD(List(1, 2, 3, 4), 2).mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 1) {
          iter
        } else {
          Nil.iterator
        }
      }
    ).collect().foreach(println)

    //看哪个数据在哪个分区
    sc.makeRDD(List(1, 2, 3, 4)).mapPartitionsWithIndex(
      (index, iter) => {
        iter.map((index, _))
      }
    ).collect().foreach(println)

    // 算子 - flatMap
    sc.makeRDD(List(List(1, 2), List(3, 4))).flatMap(x=>x).collect().foreach(println)
    sc.makeRDD(List("hello gzh", "hello hahaha")).flatMap(_.split(" ")).collect().foreach(println)
    sc.makeRDD(List(List(1, 2), List(3, 4), 5)).flatMap {
      case list: List[_] => list
      case x => List(x)
    }.collect().foreach(println)

    // 算子 - glom
    // 将一个分区内的数据转换成Array
    val rdd3: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val glomRDD: RDD[Array[Int]] = rdd3.glom()
    glomRDD.collect().foreach(data => println(data.mkString(",")))

    sc.stop()
  }
}
