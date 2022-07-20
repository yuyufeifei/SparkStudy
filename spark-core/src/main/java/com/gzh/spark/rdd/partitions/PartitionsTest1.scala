package com.gzh.spark.rdd.partitions

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object PartitionsTest1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Partitions")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      ("scala", "xxx"),
      ("spark", "xxxx"),
      ("scala", "xxxxx"),
      ("java", "xxxxxx")
    ), 3)
    //只有 Key-Value 类型的 RDD 才有分区器
    val myPartitionRDD = rdd.partitionBy(new MyPartitioner)
    myPartitionRDD.saveAsTextFile("output")

    sc.stop()
  }

  class MyPartitioner extends Partitioner {
    //分区数量
    override def numPartitions: Int = 3

    // 根据数据的key值返回数据的分区索引（从0开始）
    override def getPartition(key: Any): Int = {
      key match {
        case "scala" => 0
        case "spark" => 1
        case _ => 2
      }
    }
  }

}
