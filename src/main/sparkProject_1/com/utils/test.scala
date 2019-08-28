package com.utils

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 测试类
  */
object test {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName( this.getClass.getName ).setMaster( "local[*]" )
    val sc = new SparkContext( conf )
    val jedis = JedisConnectionPool.getConnection()
    jedis.set("123","111")
    val str = jedis.get("123")
    println( str )
    jedis.close()
  }
}
