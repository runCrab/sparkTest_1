package com.Tags

import org.apache.spark.sql.Row

object  TagLoc extends com.utils.Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    val row = args(0).asInstanceOf[Row]

   val province =  row.getAs[String]("provincename")

    val city = row.getAs[String]("cityname")

    list:+=("ZP"+province,1)
    list:+=("ZC"+city,1)
    list
  }
}
