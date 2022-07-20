package com.gzh.spark.wc

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object WordCount4 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

//    wc1(sc)
//    wc2(sc)
//    wc3(sc)
//    wc4(sc)
//    wc5(sc)
//    wc6(sc)
//    wc7(sc)
//    wc8(sc)
//    wc9(sc)
//    wc10(sc)
    wc11(sc)

    sc.stop()
  }

  // groupBy
  def wc1(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    rdd.flatMap(_.split(" ")).groupBy(word=>word).mapValues(iter=>iter.size).foreach(println)
  }

  // groupByKey 性能不如reduceByKey
  def wc2(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    rdd.flatMap(_.split(" ")).map((_, 1)).groupByKey().mapValues(iter=>iter.size).foreach(println)
  }

  // reduceByKey
  def wc3(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).foreach(println)
  }

  // aggregateByKey
  def wc4(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    rdd.flatMap(_.split(" ")).map((_, 1)).aggregateByKey(0)(_ + _, _ + _).foreach(println)
  }

  // foldByKey
  def wc5(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    rdd.flatMap(_.split(" ")).map((_, 1)).foldByKey(0)(_ + _).foreach(println)
  }

  // combineByKey
  def wc6(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    rdd.flatMap(_.split(" ")).map((_, 1)).combineByKey(v => v, (x: Int, y) => x + y, (x: Int, y: Int) => x + y)
      .foreach(println)
  }

  // countByKey
  def wc7(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    rdd.flatMap(_.split(" ")).map((_, 1)).countByKey().foreach(println)
  }

  // countByValue
  def wc8(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    rdd.flatMap(_.split(" ")).countByValue().foreach(println)
  }

  // reduce
  def wc9(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    rdd.flatMap(_.split(" ")).map(word => mutable.Map[String, Long]((word, 1))).reduce(
      (map1, map2) => {
        map2.foreach{
          case (word, count) => map1.update(word, map1.getOrElse(word, 0L) + count)
        }
        map1
      }
    ).foreach(println)
  }

  // aggregate
  def wc10(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    rdd.flatMap(_.split(" ")).aggregate(mutable.Map[String, Long]())(
      (map, data) => {  //初始值和传入的数据做计算
        map(data) = map.getOrElse(data, 0L) + 1
        map
      },
      (map1, map2) => {
        map1.foldLeft(map2)(
          (mergedMap, kv) => {
            val key = kv._1
            val value = kv._2
            mergedMap(key) = mergedMap.getOrElse(key, 0L) + value
            mergedMap
          }
        )
      }
    ).foreach(println)
  }

  // fold
  def wc11(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("hello scala", "hello spark"))
    rdd.flatMap(_.split(" ")).map(word => mutable.Map[String, Long]((word, 1))).fold(mutable.Map[String, Long]())(
      (map1: mutable.Map[String, Long], map2: mutable.Map[String, Long]) => {
        map1.foldLeft(map2)(
          (mergedMap, kv) => {
            val key = kv._1
            val value = kv._2
            mergedMap(key) = mergedMap.getOrElse(key, 0L) + value
            mergedMap
          }
        )
      }
    ).foreach(println)
  }
}
