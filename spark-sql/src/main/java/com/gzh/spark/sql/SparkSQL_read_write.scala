package com.gzh.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import java.util.Properties

object SparkSQL_read_write {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    // spark.read.load 是加载数据的通用方法
    /*
      spark.read.
      csv format jdbc json load option options orc parquet schema table text textFile

      spark.read.format("…")[.option("…")].load("…")
      format("…")：指定加载的数据类型，包括"csv"、"jdbc"、"json"、"orc"、"parquet"和"textFile"。
      load("…")：在"csv"、"jdbc"、"json"、"orc"、"parquet"和"textFile"格式下需要传入加载数据的路径。
      option("…")：在"jdbc"格式下需要传入 JDBC 相应参数，url、user、password 和 dbtable
     */

    // 文件格式.`文件路径`
    spark.sql("select * from json.`/data/user.json`").show

    // df.write.save 是保存数据的通用方法
    /*
      df.write.
      csv jdbc json orc parquet textFile… …

      df.write.format("…")[.option("…")].save("…")
      format("…")：指定保存的数据类型，包括"csv"、"jdbc"、"json"、"orc"、"parquet"和"textFile"。
      save ("…")：在"csv"、"orc"、"parquet"和"textFile"格式下需要传入保存数据的路径。
      option("…")：在"jdbc"格式下需要传入 JDBC 相应参数，url、user、password 和 dbtable

      保存操作可以使用 SaveMode, 用来指明如何处理数据，使用 mode()方法来设置。但这些 SaveMode 都是没有加锁的, 也不是原子操作。
      SaveMode 是一个枚举类，其中的常量包括："error"(default)、"append"、"overwrite"、"ignore"
      df.write.mode("append").json("/opt/module/data/output")
     */


    // Parquet  Spark SQL 的默认数据源为 Parquet 格式
    val df = spark.read.load("examples/src/main/resources/users.parquet")
    df.write.mode("append").save("/opt/module/data/output")

    // JSON   Spark SQL能够自动推测JSON数据集的结构，并将它加载为一个 Dataset[Row]
    // Spark 读取的 JSON 文件不是传统的 JSON 文件，每一行都应该是一个 JSON 串
    spark.read.json("path")

    // CSV
    spark.read.format("csv")
      .option("sep", ";").option("inferSchema", "true").option("header", "true")
      .load("data/user.csv")

    // MySQL
    //方式 1：通用的 load 方法读取
    spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark-sql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user")
      .load().show
    //方式 2:通用的 load 方法读取 参数另一种形式
    spark.read.format("jdbc")
      .options(Map(
        "url"->"jdbc:mysql://localhost:3306/spark-sql?user=root&password=123456",
        "dbtable"->"user",
        "driver"->"com.mysql.jdbc.Driver"))
      .load().show
    //方式 3:使用 jdbc 方法读取
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123345")
    spark.read.jdbc("jdbc:mysql://localhost:3306/spark-sql","user", props).show

    val ds: Dataset[User] = spark.sparkContext.makeRDD(List(User("lisi", 20), User("zs", 30))).toDS
    //方式 1：通用的方式 format 指定写出类型
    ds.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark-sql")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user")
      .mode(SaveMode.Append)
      .save()
    //方式 2：通过 jdbc 方法
    val props1: Properties = new Properties()
    props1.setProperty("user", "root")
    props1.setProperty("password", "123456")
    ds.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/spark-sql","user", props1)


    spark.stop()
  }
  case class User(name: String, age: Long)
}
