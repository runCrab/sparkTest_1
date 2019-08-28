package com.utils


object httpTest {
  def main(args: Array[String]): Unit = {
    val jsonstr = HttpUtil.get( "https://restapi.amap.com/v3/geocode/regeo?location=116.310006,39.991957&key=526cd8b243182f7e00bd2784edcc4a96" )
   println(jsonstr)
  }

}
