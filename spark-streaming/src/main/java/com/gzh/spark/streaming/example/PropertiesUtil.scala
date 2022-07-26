package com.gzh.spark.streaming.example

import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {
  def load(propertiesName: String): Properties = {
    val prop = new Properties()
    prop.load(new InputStreamReader(
     Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),
     "UTF-8")
    )
    prop
  }
}
