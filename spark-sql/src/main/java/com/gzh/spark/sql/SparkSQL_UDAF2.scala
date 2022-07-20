package com.gzh.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

object SparkSQL_UDAF2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = spark.read.json("data/test.json")
    df.createOrReplaceTempView("user")

    spark.udf.register("avgAge", functions.udaf(new MyAvgUDAF))

    spark.sql("select avgAge(age) from user").show

    spark.stop()
  }
  /* 结果
    +--------------+
    |myavgudaf(age)|
    +--------------+
    |            40|
    +--------------+
   */

  /**
   * 强类型
   */
  class MyAvgUDAF extends Aggregator[Long, Buff, Long] {

    // 初始值，缓冲区初始化
    override def zero: Buff = Buff(0L, 0L)

    // 根据输入的数据更新缓冲区的数据
    override def reduce(buff: Buff, in: Long): Buff = {
      buff.sum = buff.sum + in
      buff.count = buff.count +1
      buff
    }

    // 合并缓冲区
    override def merge(buff1: Buff, buff2: Buff): Buff = {
      buff1.sum = buff1.sum + buff2.sum
      buff1.count = buff1.count + buff2.count
      buff1
    }

    // 计算结果
    override def finish(reduction: Buff): Long = reduction.sum / reduction.count

    // 缓冲区的编码操作
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    // 输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

  case class Buff (var sum: Long, var count: Long)

}
