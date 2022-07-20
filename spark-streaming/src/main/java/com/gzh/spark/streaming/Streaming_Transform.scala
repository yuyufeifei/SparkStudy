package com.gzh.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming_Transform {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamingWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lineStreams = ssc.socketTextStream("node1", 9999)

    // transform应用场景：
    // 1. DStream功能不完善
    // 2. 需要代码周期性执行

    // Code: Driver端
    lineStreams.transform(
      rdd => {
        // Code: Driver端，（周期性执行）
        rdd.map(
          str => {
            // Code: Executor端
            str
          }
        )
      }
    )

    // Code: Driver端
    lineStreams.map(
      data => {
        // Code: Executor
        data
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
