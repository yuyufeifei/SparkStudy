package com.gzh.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming_Join {
  def main(args: Array[String]): Unit = {
    //1.创建 SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JoinTest")
    //2.创建 StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //3.从端口获取数据创建流
    val lineDStream1: ReceiverInputDStream[String] = ssc.socketTextStream("node1", 9999)
    val lineDStream2: ReceiverInputDStream[String] = ssc.socketTextStream("node2", 8888)
    //4.将两个流转换为 KV 类型
    val wordToOneDStream: DStream[(String, Int)] = lineDStream1.flatMap(_.split(" ")).map((_, 1))
    val wordToADStream: DStream[(String, String)] = lineDStream2.flatMap(_.split(" ")).map((_, "a"))
    //5.流的 JOIN
    // DStream的join操作，其实就是两个RDD的join
    val joinDStream: DStream[(String, (Int, String))] = wordToOneDStream.join(wordToADStream)
    //6.打印
    joinDStream.print()
    //7.启动任务
    ssc.start()
    ssc.awaitTermination()
  }
}
