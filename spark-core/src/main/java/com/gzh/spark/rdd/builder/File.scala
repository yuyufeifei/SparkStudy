package com.gzh.spark.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * 从外部存储（文件）创建 RDD
 */
object File {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //textFile以行为单位读取数据
//    val rdd: RDD[String] = sc.textFile("D:\\GZH\\IdeaProjects\\SparkStudy\\data\\1.txt")    //全路径
//    val rdd: RDD[String] = sc.textFile("data/1.txt")    //绝对路径
//    val rdd: RDD[String] = sc.textFile("data")    //目录
//    val rdd: RDD[String] = sc.textFile("data/*.txt")    //通配符
//    val rdd: RDD[String] = sc.textFile("hdfs://")    //hdfs路径 hdfs://node1:8020/data  hdfs://mycluster/data

    //wholeTextFiles以文件为单位读取数据
    val rdd1 = sc.wholeTextFiles("data")

//    rdd.collect().foreach(println)
    rdd1.collect().foreach(println)

    sc.stop()
  }
}
