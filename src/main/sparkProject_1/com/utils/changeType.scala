package com.utils
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.rdd.RDD

object changeType {

  def changes(df:DataFrame):RDD[(Int, Int, Int, Int, Int, Int, Int, Double, Double)] ={
     val value =df.rdd.map(row => {
      val requestmode = row.getAs[Int]( "requestmode" )
      val processnode = row.getAs[Int]( "processnode" )
      val iseffective = row.getAs[Int]( "iseffective" )
      val isbilling = row.getAs[Int]( "isbilling" )
      val isbid = row.getAs[Int]( "isbid" )
      val iswin = row.getAs[Int]( "iswin" )
      val adorderid = row.getAs[Int]( "adorderid" )
      val WinPrice = row.getAs[Double]( "winprice" )
      val adpayment = row.getAs[Double]( "adpayment" )
      (requestmode, processnode, iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)
    } )
    value
  }

}
