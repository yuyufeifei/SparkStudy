package com.gzh.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/**
 * 向端口发送数据：nc -lk 9999
 */
object Streaming_DIY {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Streaming_DIY")
    val ssc = new StreamingContext(conf, Seconds(4))

    val msgDS: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver())
    msgDS.print()

    ssc.start()
    ssc.awaitTermination()
  }

  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    private var flag = true
    override def onStart(): Unit = {
      new Thread(() => {
        while (flag) {
          val message = "采集的数据为：" + new Random().nextInt(10).toString
          store(message)
          Thread.sleep(500)
        }
      }).start()
    }

    override def onStop(): Unit = {
      flag = false
    }
  }
}
