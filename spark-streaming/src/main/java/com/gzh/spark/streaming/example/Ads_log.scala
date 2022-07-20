package com.gzh.spark.streaming.example

case class Ads_log(timestamp: Long,
 area: String,
 city: String,
 userid: String,
 adid: String)