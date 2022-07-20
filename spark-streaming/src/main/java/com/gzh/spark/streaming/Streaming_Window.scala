package com.gzh.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming_Window {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamingWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lineStreams = ssc.socketTextStream("node1", 9999)

    val wordToOne = lineStreams.map((_, 1))

    // 窗口的范围应该是采集周期的整数倍
    // 可能会出现重复计算，可以改变滑动的步长(添加第二个参数)来避免这种情况
//    val windowDS: DStream[(String, Int)] = wordToOne.window(Seconds(6))
    val windowDS: DStream[(String, Int)] = wordToOne.window(Seconds(6), Seconds(6))

    val wordCount: DStream[(String, Int)] = windowDS.reduceByKey(_ + _)

    wordCount.print()


    ssc.start()
    ssc.awaitTermination()
  }
}
