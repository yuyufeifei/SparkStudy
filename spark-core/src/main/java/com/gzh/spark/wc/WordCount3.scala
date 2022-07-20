package com.gzh.spark.wc

import org.apache.spark.{SparkConf, SparkContext}

object WordCount3 {
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
    //spark
    //reduceByKey 相同key的数据，可以对value进行reduce
    val wordCount = wordToOne.reduceByKey(_+_)
    //将结果打印
    val array = wordCount.collect()
    array.foreach(println)

    sc.stop()
  }
}
