package com.gzh.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Streaming_Window1 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamingWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("./checkpoint")

    val lineStreams = ssc.socketTextStream("node1", 9999)

    val wordToOne = lineStreams.map((_, 1))

    // 当窗口范围比较大，但滑动幅度比较小的情况，可以采用增加数据和删除数据的方式，避免重复计算，提升性能
    val windowDS: DStream[(String, Int)] = wordToOne.reduceByKeyAndWindow(
      (x: Int, y: Int) => {x + y},
      (x: Int, y: Int) => {x - y},
      Seconds(9),
      Seconds(3)
    )

    windowDS.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
