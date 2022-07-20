package com.gzh.spark.streaming.example

/**
 *
 * 城市信息表
 *
 * @param city_id 城市 id
 * @param city_name 城市名称
 * @param area 城市所在大区
 */
case class CityInfo (city_id:Long,
                     city_name:String,
                     area:String)