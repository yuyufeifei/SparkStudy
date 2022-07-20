package com.gzh.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * 向端口发送数据：nc -lk 9999
 */
object Streaming_Queue {
  def main(args: Array[String]): Unit = {
    //1.初始化 Spark 配置信息
    val conf = new SparkConf().setMaster("local[*]").setAppName("Streaming_Queue")
    //2.初始化 SparkStreamingContext
    val ssc = new StreamingContext(conf, Seconds(4))
    //3.创建 RDD 队列
    val rddQueue = new mutable.Queue[RDD[Int]]()
    //4.创建 QueueInputDStream
    val inputStream = ssc.queueStream(rddQueue,oneAtATime = false)
    //5.处理队列中的 RDD 数据
    val mappedStream = inputStream.map((_,1))
    val reducedStream = mappedStream.reduceByKey(_ + _)
    //6.打印结果
    reducedStream.print()
    //7.启动任务
    ssc.start()
    //8.循环创建并向 RDD 队列中放入 RDD
    for (_ <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }
    ssc.awaitTermination()
  }
}
