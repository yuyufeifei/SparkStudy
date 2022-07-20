package com.gzh.spark.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 从外部存储（文件）创建 RDD
 */
object File_partition {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    // textFile的第二个参数表示分区的数量
    // Spark读取文件，底层使用的是Hadoop的读取方式
    // 2.txt共7个字符（换行占两个），每个分区3字符，最后分区数为3
    // 数据以行为单位进行读取，读取时以偏移量为单位
    // 三个分区以[0,3],[3,6],[6,7]读取，数字代表偏移量，左右包含。
    // [0,3]读完后，第一个分区里的数据为“1换行2”，读[3,6]时，因为“2那行读过了”，所以从第三行开始读，第二个分区里是“3换行”
    // 如果数据源为多个文件，计算分区时以文件为单位
    val rdd: RDD[String] = sc.textFile("data/2.txt", 2)

    rdd.saveAsTextFile("output")

    sc.stop()
  }
}
