package com.gzh.spark.wc

import org.apache.spark.{SparkConf, SparkContext}

object WordCount2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //读取文件，获得一行一行的数据
    val lines = sc.textFile("data")
    //将数据拆分成一个一个的单词 "hello scala"=>"hello"、"scala"
    val words = lines.flatMap(_.split(" "))
    val wordToOne = words.map(
      word => (word, 1)
    )
    val wordGroup = wordToOne.groupBy(t => t._1)
    wordGroup.foreach(println)
    val wordCount = wordGroup.map {
      case (_, list) =>
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
    }
    //将结果打印
    val array = wordCount.collect()
    array.foreach(println)

    sc.stop()
  }
}
