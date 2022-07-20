package com.gzh.spark.rdd.operator.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object OperatorTransform4 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(1, 2, 3, 4), 2)
    val rdd2 = sc.makeRDD(List(("a", 1), ("a", 2), ("b", 1), ("c", 1), ("b", 3)))
    val rdd3 = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)), 2)

    // 算子 - （Key-Value）类型

    // partitionBy：根据指定的分区规则对数据进行重分区
    rdd1.map((_, 1)).partitionBy(new HashPartitioner(2))
//      .saveAsTextFile("output")

    // reduceByKey：相同key的数据进行value数据的聚合操作
    // 支持分区内预聚合（combine）功能，可以有效减少shuffle时落盘的数据量，提升shuffle的性能
    // 分区内和分区间计算规则相同
    rdd2.reduceByKey(_+_)
      .collect().foreach(println)

    // groupByKey：将数据源的数据根据key对value进行分组
    rdd2.groupByKey()
      .collect().foreach(println)

    // aggregateByKey：将数据根据不同的规则进行分区内计算和分区间计算
    /*取出每个分区内相同 key 的最大值然后分区间相加
    aggregateByKey 算子是函数柯里化，存在两个参数列表
    1. 第一个参数列表中的参数表示初始值，主要用于当碰见第一个key的时候，和value进行分区内计算。最终返回的数据结果和初始值的类型一致
    2. 第二个参数列表中含有两个参数
    2.1 第一个参数表示分区内的计算规则
    2.2 第二个参数表示分区间的计算规则*/
    rdd3.aggregateByKey(0)(math.max, _+_)
      .collect().foreach(println)

    // 得到每个key的平均值
    sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)
      .aggregateByKey((0, 0))(
        (t, v) => {(t._1 + v, t._2 + 1)}, // t代表初始值(0, 0)，v代表第一个元素("a", 1)的value值，即1
        (t1, t2) => {(t1._1 + t2._1, t1._2 + t2._2)}  //t1代表第一个分区计算之后的值，t2代表第二个分区计算之后的值
      ) // 计算之后得到 (a,(9,3))，(b,(12,3))
      .mapValues{
        case (total, num) => total / num
      } // 计算之后得到(a,3)，(b,4)
      .collect().foreach(println)

    // foldByKey：当分区内计算规则和分区间计算规则相同时，aggregateByKey 就可以简化为 foldByKey
    rdd3.foldByKey(0)(_+_)
      .collect().foreach(println)

    // combineByKey：最通用的对 key-value 型 rdd 进行聚集操作的聚集函数
    // 得到每个key的平均值
    sc.makeRDD(List(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)), 2)
      .combineByKey(
        v => (v, 1),    // 将相同key的第一个数据进行结构转换
        (t: (Int, Int), v) => {(t._1 + v, t._2 + 1)}, // 分区内计算：t代表转换后的值(1, 1)，第一个1为第一个元素的value值，v代表第二个元素的value值
        (t1:(Int, Int), t2:(Int, Int)) => {(t1._1 + t2._1, t1._2 + t2._2)}  // 分区间计算 ：t1代表第一个分区计算之后的值，t2代表第二个分区计算之后的值
      ) // 计算之后得到 (a,(9,3))，(b,(12,3))
      .mapValues{
        case (total, num) => total / num
      } // 计算之后得到(a,3)，(b,4)
      .collect().foreach(println)

    /**
     * reduceByKey: 相同 key 的第一个数据不进行任何计算，分区内和分区间计算规则相同
     * foldByKey: 相同 key 的第一个数据和初始值进行分区内计算，分区内和分区间计算规则相同
     * aggregateByKey：相同 key 的第一个数据和初始值进行分区内计算，分区内和分区间计算规则可以不相同
     * combineByKey: 当计算时，发现数据结构不满足要求时，可以让第一个数据转换结构。分区内和分区间计算规则不相同。
     */

    // sortByKey 默认升序
    rdd2.sortByKey()
      .collect().foreach(println)

    // join 在类型为(K,V)和(K,W)的 RDD 上调用，返回一个相同 key 对应的所有元素连接在一起的(K,(V,W))的 RDD
    val rdd4 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3), ("d", 4)))
    val rdd5 = sc.makeRDD(List(("a", "x"), ("b", "y"), ("c", "z"), ("d", "o")))
    rdd4.join(rdd5)
      .collect().foreach(println)
    /* 结果
    (a,(1,x))
    (b,(2,y))
    (c,(3,z))
    (d,(4,o))
    */

    val rdd6 = sc.makeRDD(List(("a", 1), ("a", 2), ("c", 3), ("d", 4)))
    val rdd7 = sc.makeRDD(List(("a", "x"), ("a", "y"), ("c", "z"), ("d", "o")))
    rdd6.join(rdd7)
      .collect().foreach(println)
    /* 结果
    (a,(1,x))
    (a,(1,y))
    (a,(2,x))
    (a,(2,y))
    (c,(3,z))
    (d,(4,o))
    */

    // leftOuterJoin
    val rdd8 = sc.makeRDD(List(("a", "x"), ("b", "y"), ("c", "z")))
    rdd4.leftOuterJoin(rdd8)
      .collect().foreach(println)
    /* 结果
    (a,(1,Some(x)))
    (b,(2,Some(y)))
    (c,(3,Some(z)))
    (d,(4,None))
    */

    // rightOuterJoin
    rdd8.rightOuterJoin(rdd4)
      .collect().foreach(println)
    /* 结果
    (a,(Some(x),1))
    (b,(Some(y),2))
    (c,(Some(z),3))
    (d,(None,4))
    */

    // cogroup 分组，连接 在类型为(K,V)和(K,W)的 RDD 上调用，返回一个(K,(Iterable<V>,Iterable<W>))类型的 RDD
    val rdd9 = sc.makeRDD(List(("a", "x"), ("b", "y"), ("c", "z"), ("c", "o")))
    rdd4.cogroup(rdd9)
      .collect().foreach(println)
    /* 结果
    (a,(CompactBuffer(1),CompactBuffer(x)))
    (b,(CompactBuffer(2),CompactBuffer(y)))
    (c,(CompactBuffer(3),CompactBuffer(z, o)))
    (d,(CompactBuffer(4),CompactBuffer()))
    */

    val rdd10 = sc.makeRDD(List(("a", "m"), ("b", "n"), ("b", "p"), ("d", "q")))
    val rdd11 = sc.makeRDD(List(("a", "aa"), ("b", "bb"), ("d", "dd")))
    rdd4.cogroup(rdd9, rdd10, rdd11)
      .collect().foreach(println)
    /* 结果
    (a,(CompactBuffer(1),CompactBuffer(x),CompactBuffer(m),CompactBuffer(aa)))
    (b,(CompactBuffer(2),CompactBuffer(y),CompactBuffer(n, p),CompactBuffer(bb)))
    (c,(CompactBuffer(3),CompactBuffer(z, o),CompactBuffer(),CompactBuffer()))
    (d,(CompactBuffer(4),CompactBuffer(),CompactBuffer(q),CompactBuffer(dd)))
    */

    sc.stop()
  }
}
