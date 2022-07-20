package com.gzh.spark.wc

object WCTest {
  def main(args: Array[String]): Unit = {
    // 单词计数：将集合中出现的相同的单词，进行计数，取计数排名前三的结果
    val stringList = List("Hello Scala Hbase kafka", "Hello Scala Hbase", "Hello Scala", "Hello")
    // 1) 将每一个字符串转换成一个一个单词
    val wordList: List[String] = stringList.flatMap(_.split(" "))
    //println(wordList)
    // 2) 将相同的单词放置在一起
    val wordToWordsMap: Map[String, List[String]] = wordList.groupBy(word => word)
    //println(wordToWordsMap)
    // 3) 对相同的单词进行计数
    // (word, list) => (word, count)
    val wordToCountMap: Map[String, Int] = wordToWordsMap.map(tuple => (tuple._1, tuple._2.size))
    // 4) 对计数完成后的结果进行排序（降序）
    val sortList: List[(String, Int)] = wordToCountMap.toList.sortWith( _._2> _._2)
    // 5) 对排序后的结果取前 3 名
    val resultList: List[(String, Int)] = sortList.take(3)
    println(resultList)
  }
}
