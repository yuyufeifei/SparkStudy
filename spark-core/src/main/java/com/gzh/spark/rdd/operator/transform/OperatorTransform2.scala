package com.gzh.spark.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object OperatorTransform2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    // 算子 - groupBy
    // 相同的key值放在同一组，数据经过groupBy后，分区不变，但数据会被打乱重新组合。
    rdd.groupBy( _ % 2 )
      .collect().foreach(println)

    // 算子 - filter
    // 过滤数据，数据经过filter后，分区不变，但分区内的数据可能不均衡，生产环境下，可能出现数据倾斜。
    rdd.filter(_ % 2 == 0)
      .collect().foreach(println)

    // 算子 - sample
    // 根据指定的规则从数据集中抽取数据
    /*抽取数据不放回（伯努利算法）
    伯努利算法：又叫 0、1 分布。例如扔硬币，要么正面，要么反面。
    具体实现：根据种子和随机算法算出一个数和第二个参数设置几率比较，小于第二个参数要，大于不要
    第一个参数：抽取的数据是否放回，false：不放回
    第二个参数：抽取的几率，范围在[0,1]之间,0：全不取；1：全取；
    第三个参数：随机数种子。如果不传，则使用系统时间。
    抽取数据放回（泊松算法）
    第一个参数：抽取的数据是否放回，true：放回；false：不放回
    第二个参数：重复数据的几率，范围大于等于 0.表示每一个元素被期望抽取到的次数
    第三个参数：随机数种子*/
    rdd1.sample(withReplacement = false, 0.5)
      .collect().foreach(println)
    rdd1.sample(withReplacement = true, 2)
      .collect().foreach(println)

    // 算子 - distinct
    // 去重 底层处理：map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
    sc.makeRDD(List(1, 2, 1, 2, 3, 4))
      .distinct()
      .collect().foreach(println)

    // 算子 - coalesce
    // 根据数据量缩减分区，用于大数据集过滤后，提高小数据集的执行效率。
    // 第二个参数表示是否进行shuffle处理，如果为false，则不处理，分区里的数据不会被打乱，可能导致数据不均衡，出现数据倾斜；
    // 如果为true，则进行处理，分区的数据会被打乱。
    // 也可实现扩大分区的效果，但第二个参数必须为true，否则不起作用。
    sc.makeRDD(List(1, 2, 3, 4, 5, 6), 6)
      .coalesce(2)
//      .saveAsTextFile("output")

    // 算子 - repartition
    // 缩减或扩大分区。底层处理：coalesce(numPartitions, shuffle = true)
    sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
      .repartition(6)
//      .saveAsTextFile("output")

    // 算子 - sortBy
    /*排序数据。在排序之前，可以将数据通过 f 函数进行处理，之后按照 f 函数处理的结果进行排序，默认为升序排列。第二个参数可以改变排序方式。
    排序后新产生的 RDD 的分区数与原 RDD 的分区数一致。中间存在 shuffle 的过程*/
    sc.makeRDD(List(6, 2, 4, 3, 5, 1), 2)
      .sortBy(x => x)
//      .saveAsTextFile("output")

    sc.stop()
  }
}
