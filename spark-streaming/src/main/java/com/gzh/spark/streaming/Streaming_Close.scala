package com.gzh.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object Streaming_Close {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamingClose")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true") // 不确定是否需要
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lineStreams = ssc.socketTextStream("node1", 9999)

    val wordToOne = lineStreams.map((_, 1))

    wordToOne.print()

    ssc.start()

    // 如果想要关闭采集器，需要创建新的线程
    new Thread(
      () => {
        // 优雅的关闭，第二个参数设为true
        // 计算节点不再接收新的数据，而是将现有的数据处理完毕，然后关闭
        // 在第第三方程序中增加关闭状态：MySQL、Redis、ZooKeeper、HDFS
        while (true) {
          if (true) { //从第三方程序中获取状态
            if (ssc.getState() == StreamingContextState.ACTIVE) {
              ssc.stop(stopSparkContext = true, stopGracefully = true)
              System.exit(0)
            }
          }
          Thread.sleep(5000)
        }
      }
    ).start()

    ssc.awaitTermination()

  }
}
