package com.Rpt

import java.util.Properties

import com.typesafe.config.ConfigFactory
import com.utils.RptUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable

object medimRpt {

  def main(args: Array[String]): Unit = {


    System.setProperty( "hadoop.home.dir", "D:\\Hadoop\\hadoop-2.7.6" )
    // 判断路径是否正确
    //    if(args.length != 1){
    //      println("目录参数不正确，退出程序")
    //      sys.exit()
    //    }
    // 创建一个集合保存输入和输出目录
    val inputPath = "D:\\ideaProject\\sparkTest_1\\parquet_out"
    val conf = new SparkConf().setAppName( this.getClass.getName ).setMaster( "local[*]" )
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
    // 创建执行入口
    val sc = new SparkContext( conf )
    val sQLContext = new SQLContext( sc )
    // 获取数据
    val df = sQLContext.read.parquet( inputPath )

    import sQLContext.implicits._

    val value = sc.textFile( "C:\\Users\\88220\\Desktop\\spark/app_dict.txt" ).filter(_.size>1)
      .map( x => {
        val arr = x.split( "\t" )
        ( arr( 1 ) , arr( 4 ))
      } ).collect()


    var map :mutable.Map[String,String] = mutable.Map()

    value.map(x=>{
      map.update(x._1,x._2)
    })

    val broadcast: Broadcast[mutable.Map[String, String]] = sc.broadcast(map)

    val medimtmp: RDD[((String), List[Double])] = df.rdd.map( row => {
      val requestmode = row.getAs[Int]( "requestmode" )
      val processnode = row.getAs[Int]( "processnode" )
      val iseffective = row.getAs[Int]( "iseffective" )
      val isbilling = row.getAs[Int]( "isbilling" )
      val isbid = row.getAs[Int]( "isbid" )
      val iswin = row.getAs[Int]( "iswin" )
      val adorderid = row.getAs[Int]( "adorderid" )
      val WinPrice = row.getAs[Double]( "winprice" )
      val adpayment = row.getAs[Double]( "adpayment" )

      val appnametmp=row.getAs[String]("appname")
      val appid = row.getAs[String]("appid")
      var appname = ""
      if(appnametmp.size<0||appname == appid){
        appname= broadcast.value.getOrElse(appid,"未知")
      }else{
        appname=appnametmp
      }
      //-----------------------------------------------------
      val requstList: List[Double] = RptUtils.request( requestmode, processnode )
      val clickList: List[Double] = RptUtils.click( requestmode, iseffective )
      val adList = RptUtils.Ad( iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment )
      ((appname), requstList ++ clickList ++ adList)
    } ).reduceByKey( (list1, list2) => {
      list1.zip( list2 ).map( l => {
        l._1 + l._2
      } )
    } )

    val resultMedim = medimtmp.map( x => {
      val appname =x._1
      val requestmode = x._2( 0 ).toInt
      val processnode = x._2( 1 ).toInt
      val iseffective = x._2( 2 ).toInt
      val clicknum = x._2( 3 ).toInt
      val shownumber = x._2( 4 ).toInt
      val countbilling = x._2( 5 ).toInt
      val iswin = x._2( 6 ).toInt
      val WinPrice = x._2( 7 )
      val adpayment = x._2( 8 )
      var rateOfbillingSuccess = 0
      try{
        rateOfbillingSuccess = iswin/countbilling
      }catch {
        case exception: Exception=> rateOfbillingSuccess
      }
      var  rateOfClick = 0
      try{
        rateOfClick = clicknum/shownumber
      }catch {
        case exception: Exception=> rateOfClick
      }
      (appname, requestmode, processnode, iseffective, countbilling, iswin, rateOfbillingSuccess,
        shownumber, clicknum, rateOfClick, WinPrice, adpayment)
    } ).toDF( "appname", "requestmode", "processnode", "iseffective", "countbilling", "iswin", "rateOfbillingSuccess",
      "shownumber", "clicknum", "rateOfClick", "WinPrice", "adpayment" )

    resultMedim.show()

    val load = ConfigFactory.load()
    val prop = new Properties()
    prop.put("user",load.getString("jdbc.user"))
    prop.put("password",load.getString("jdbc.password"))
    resultMedim.write.jdbc(load.getString("jdbc.url"),load.getString("jdbc.TableNameOfMedim"),prop)
    sc.stop()

  }
}