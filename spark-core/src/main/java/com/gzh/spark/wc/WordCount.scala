package com.gzh.spark.wc

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //读取文件，获得一行一行的数据
    val lines = sc.textFile("data")
    //将数据拆分成一个一个的单词 "hello scala"=>"hello"、"scala"
    val words = lines.flatMap(_.split(" "))
    //将数据根据单词进行分组 (hello,hello,hello,hello),(scala,scala)
    val wordGroup = words.groupBy(word => word)
    wordGroup.foreach(println)
    //对分组后的数据进行转换 (hello,4),(scala,2)
    val wordCount = wordGroup.map {
      case (word, list) =>
        (word, list.size)
    }
    //将结果打印
    val array = wordCount.collect()
    array.foreach(println)

    sc.stop()
  }
}
