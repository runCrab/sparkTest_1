package com.Tags

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.Row
import project_01.Utils2Type
import com.utils._
import org.apache.commons.lang3.StringUtils


object BusinessTag extends com.utils.Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    //把读入的数据转化为row类型
    val row = args( 0 ).asInstanceOf[Row]
    //分别拿到数据中的long和lat信息
    val long = Utils2Type.toDouble( row.getAs[String]( "long" ) )
    val lat = Utils2Type.toDouble( row.getAs[String]( "lat" ) )

    //中国的经纬度范围大约为：纬度3.86~53.55，经度73.66~135.05, 不在范围类的数据可需要处理.
    if (long > 3 && long < 54
      && lat > 73 && lat < 136) {
      val BusInfo: String = getBussinessInfo( long, lat )
      val arr: Array[String] = BusInfo.split(",")
      arr.foreach(f=>list :+= (f, 1))
      println("makeTags")
      arr.foreach(println)
    }
    list
  }



  private def getBussinessInfo(long: Double, lat: Double): String = {

    //转换GeoHash进行判断
    val geoForJedisKey: String = GeoHash.geoHashStringWithCharacterPrecision( lat, long, 8 )
    val jedis = JedisConnectionPool.getConnection()
    //从jedis中获取数据
    val jeidsRes = jedis.get( geoForJedisKey )

    println("getBussinessInfo")
    println(jedis.toString)
    var resOfBuss = ""
    resOfBuss = jeidsRes
    //判断返回的数据是否为空
    if (StringUtils.isNotBlank( jeidsRes )) { //当返回数据为空时，说明数据在jedis中不存在，
      //需要从地图API中获取商圈信息，并且存入到redis数据库中。
      val resOfBuss: String = com.utils.AmapUtil.getBusinessFromAmap( long, lat )
      println(resOfBuss)
      //存储到redis
      jedis.set( geoForJedisKey, resOfBuss )
      println(geoForJedisKey,resOfBuss)
      jedis.close()
    }
    resOfBuss
  }
}
