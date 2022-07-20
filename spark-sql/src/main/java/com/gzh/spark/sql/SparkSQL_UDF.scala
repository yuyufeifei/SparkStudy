package com.gzh.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL_UDF {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.read.json("data/test.json")
    df.createOrReplaceTempView("user")

    spark.udf.register("prefixName", "name: " + _)

    spark.sql("select age,prefixName(username) from user").show

    spark.stop()
  }
}
