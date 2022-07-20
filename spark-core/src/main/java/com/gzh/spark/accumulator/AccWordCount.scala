package com.gzh.spark.accumulator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

object AccWordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("AccWordCount")
    val sc = new SparkContext(sparkConf)
    val rdd = sc.makeRDD(List("hello", "word", "hello", "spark"))

    // 创建累加器对象
    val wcAcc = new WordCountAccumulator()

    // 向spark注册
    sc.register(wcAcc, "AccWordCount")

    rdd.foreach(
      word => {
        // 使用累加器
        wcAcc.add(word)
      }
    )

    // 获取累加器的结果
    println(wcAcc.value)

    sc.stop()
  }
}
// 自定义累加器
// 1. 继承 AccumulatorV2，并设定泛型：IN:累加器输入的数据类型，OUT:累加器返回的数据类型
// 2. 重写累加器的抽象方法
class WordCountAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]]{

  var map : mutable.Map[String, Long] = mutable.Map()

  // 累加器是否为初始状态
  override def isZero: Boolean = {
    map.isEmpty
  }
  // 复制累加器
  override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
    new WordCountAccumulator
  }
  // 重置累加器
  override def reset(): Unit = {
    map.clear()
  }
  // 向累加器中增加数据 (In)
  override def add(word: String): Unit = {
    // 查询 map 中是否存在相同的单词
    // 如果有相同的单词，那么单词的数量加 1
    // 如果没有相同的单词，那么在 map 中增加这个单词
    map(word) = map.getOrElse(word, 0L) + 1L
  }
  // 合并累加器
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
    val map1 = map
    val map2 = other.value
    // 两个 Map 的合并
    map = map1.foldLeft(map2)(
      (mergedMap, kv ) => {
        mergedMap(kv._1) = mergedMap.getOrElse(kv._1, 0L) + kv._2
        mergedMap
      }
    )
  }
  // 返回累加器的结果 （Out）
  override def value: mutable.Map[String, Long] = map
}
