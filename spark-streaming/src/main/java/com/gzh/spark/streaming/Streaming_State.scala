package com.gzh.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 向端口发送数据：nc -lk 9999
 */
object Streaming_State {
  def main(args: Array[String]): Unit = {
    //1.初始化 Spark 配置信息
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("StreamingWordCount")
    //2.初始化 SparkStreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("./checkpoint")

    // 无状态数据操作，只对当前的采集周期内的数据进行处理
    // 某些场合下，需要保留数据统计结果（状态），实现数据的汇总
    // 使用有状态操作时，需要设置检查点路径ssc.checkpoint
    val lineStreams = ssc.socketTextStream("node1", 9999)

    val wordAndOneStreams = lineStreams.map((_, 1))

    // 根据key对数据的状态进行更新
    // 第一个参数表示相同key的value数据，第二个参数表示缓存区相同key的value数据
    val wordAndCountStreams = wordAndOneStreams.updateStateByKey(
      (seq: Seq[Int], buff: Option[Int]) => {
        val newCount = buff.getOrElse(0) + seq.sum
        Option(newCount)
      }
    )
    //打印
    wordAndCountStreams.print()
    //启动 SparkStreamingContext
    ssc.start()
    ssc.awaitTermination()
//    ssc.stop()
  }
}
