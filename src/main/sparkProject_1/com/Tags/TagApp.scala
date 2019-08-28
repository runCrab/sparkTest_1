package com.Tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import redis.clients.jedis.Jedis


object TagApp extends com.utils.Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()
    val row  = args(0).asInstanceOf[Row]
    val AppName: String = row.getAs[String]("appname")

    val appid = row.getAs[String]("appid")

    if(StringUtils.isNotBlank(AppName)){
      list:+=("APP"+AppName,1)
    }else if(StringUtils.isBlank(AppName)){
      var jedis = new Jedis("NODE03")
      val tmp = jedis.get(appid)
      list:+=("appid"+tmp,1)
    }

    list
  }
}
