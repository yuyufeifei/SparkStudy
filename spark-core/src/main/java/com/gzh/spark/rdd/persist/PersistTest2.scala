package com.gzh.spark.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PersistTest2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Persist")
    val sc = new SparkContext(sparkConf)
    // 设置检查点路径
    sc.setCheckpointDir("checkpoint")

    val rdd = sc.makeRDD(List("hello scala", "hello spark"))

    val wordRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = wordRDD.map(
      word => {
        println("map")
        (word, 1)
      }
    )

    // 需要指定检查点保存路径
    // 一般保存在分布式系统中，如HDFS
    // 建议对checkpoint()的RDD使用Cache缓存，这样checkpoint的job只需从Cache缓存中读取数据即可，否则需要再从头计算一次RDD
    // checkpoint会切断血缘关系，重新建立新的血缘关系
    // checkpoint等同于改变数据源
    mapRDD.cache()
    mapRDD.checkpoint()

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    reduceRDD.collect().foreach(println)

    val groupRDD = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)

    sc.stop()
  }
}
