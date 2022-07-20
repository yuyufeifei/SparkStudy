package com.gzh.spark.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PersistTest1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Persist")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List("hello scala", "hello spark"))

    val wordRDD: RDD[String] = rdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = wordRDD.map(
      word => {
        println("map")
        (word, 1)
      }
    )

    // 添加cache之后，上面的打印map只执行一次
    // cache会在血缘关系中添加新的依赖
    mapRDD.cache()
    // 默认为StorageLevel.MEMORY_ONLY
//    mapRDD.persist()

    println(mapRDD.toDebugString)

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
    reduceRDD.collect().foreach(println)

    val groupRDD = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)

    println(mapRDD.toDebugString)

    sc.stop()
  }
}
