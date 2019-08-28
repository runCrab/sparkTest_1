package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

object TagTerminal extends Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row = args( 0 ).asInstanceOf[Row]
    //a)	(操作系统 -> 1)
    //b)	(联网方 -> 1)
    //c)	(运营商 -> 1)
    val ispname = row.getAs[String]( "ispname" )//运营商
    val client = row.getAs[Int]( "client" ) //操作系统
    val networkmannername = row.getAs[String]( "networkmannername" ) //联网方式

    val clienttmp = client match {
      case 1 =>"1 Android D00010001"
      case 2=>"2 IOS D00010002"
      case 3=>"3 WinPhone D00010003"
      case _=>"_ 其 他 D00010004"
    }

    val ispnametmp = ispname match {
      case "移动" => "移 动 D00030001"
      case "联通" => "联 通 D00030002"
      case "电信" => "电 信 D00030003"
      case   _  => "_ D00030004"

    }
    val nettmp = networkmannername match {
      case "Wifi" => "WIFI D00020001 "
      case "4G" => "4G D00020002"
      case "3G" => "3G D00020003"
      case _ => "_   D00020005"
    }

    list:+=(clienttmp,1)
    list:+=(nettmp,1)
    list:+=(ispnametmp,1)

    list
  }
}
