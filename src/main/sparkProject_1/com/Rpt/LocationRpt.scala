package com.Rpt

import java.util.Properties
import com.typesafe.config.ConfigFactory
import com.utils.{RptUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}

/**
  * 地域分布指标
  */
object LocationRpt {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\Hadoop\\hadoop-2.7.6")
    // 判断路径是否正确
//    if(args.length != 1){
//      println("目录参数不正确，退出程序")
//      sys.exit()
//    }
    // 创建一个集合保存输入和输出目录
    val inputPath ="D:\\ideaProject\\sparkTest_1\\parquet_out"
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 获取数据
    import sQLContext.implicits._
    val df: DataFrame = sQLContext.read.parquet(inputPath)

    //区域
    val locationtmp: RDD[((String, String), List[Double])] = df.rdd.map( row => {
      val requestmode = row.getAs[Int]( "requestmode" )
      val processnode = row.getAs[Int]( "processnode" )
      val iseffective = row.getAs[Int]( "iseffective" )
      val isbilling = row.getAs[Int]( "isbilling" )
      val isbid = row.getAs[Int]( "isbid" )
      val iswin = row.getAs[Int]( "iswin" )
      val adorderid = row.getAs[Int]( "adorderid" )
      val WinPrice = row.getAs[Double]( "winprice" )
      val adpayment = row.getAs[Double]( "adpayment" )
      // key 值  是地域的省市
      val pro = row.getAs[String]( "provincename" )
      val city = row.getAs[String]( "cityname" )
      //-----------------------------------------------------
      val requstList: List[Double] = RptUtils.request( requestmode, processnode )
      val clickList: List[Double] = RptUtils.click( requestmode, iseffective )
      val adList = RptUtils.Ad( iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment )
      ((pro, city), requstList ++ clickList ++ adList)
    } ).reduceByKey( (list1, list2) => {
      list1.zip( list2 ).map( l => {
        l._1 + l._2
      } )
    } )


    //区域DF
     val resultOflocation = locationtmp.map( x => {
      val pro = x._1._1
      val city = x._1._2
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
      (pro, city, requestmode, processnode, iseffective, countbilling, iswin, rateOfbillingSuccess,
        shownumber, clicknum, rateOfClick, WinPrice, adpayment)
    } ).toDF( "pro", "city", "requestmode", "processnode", "iseffective", "countbilling", "iswin", "rateOfbillingSuccess",
      "shownumber", "clicknum", "rateOfClick", "WinPrice", "adpayment" )

    resultOflocation.show()


    //获取.conf文件中的JDBC配置信息
    val load = ConfigFactory.load()
    val prop = new Properties()
    prop.put("user",load.getString("jdbc.user"))
    prop.put("password",load.getString("jdbc.password"))

    resultOflocation.write.jdbc(load.getString("jdbc.url"),load.getString("jdbc.TableNameOfLoc"),prop)


//    df.createOrReplaceTempView("tmp")
//
//    //地域分布
//    sQLContext.sql("select t.provincename ,t.cityname , " +
//      "sum(totalRequest) as totalRequest, " +
//      "sum(Validrequest) as Validrequest , " +
//      "sum(adRequest) as adRequest , " +
//      "sum(countOfbid) as countOfbid, " +
//      "sum(succesOfbid) as succesOfbid , " +
//      "sum(numOfShow) as numOfShow , " +
//      "sum(numOfClick) as numOfClick , " +
//      "sum(adConsumption) as adConsumption , " +
//      "sum(adPay) as adPay  " +
//      "from " +
//      "( " +
//      "select provincename ,cityname , " +
//      "(case when requestmode = 1 and processnode>=1 then 1 else 0 end  ) as totalRequest , " +
//      "(case when requestmode = 1 and processnode>=2 then 1 else 0 end  ) as Validrequest , " +
//      "(case when requestmode = 1 and processnode =3 then 1 else 0 end  ) as adRequest , " +
//      "(case when iseffective = 1 and isbilling = 1 and isbid = 1 then 1 else 0 end  ) as countOfbid, " +
//      "(case when iseffective = 1 and isbilling = 1 and  iswin = 1 and adorderid != 0 then 1 else 0 end ) as succesOfbid , " +
//      "(case when requestmode = 2 and iseffective = 1 then 1 else 0 end ) as numOfShow , " +
//      "(case when requestmode = 3 and iseffective = 1 then 1 else 0 end ) as numOfClick , " +
//      "(case when iseffective = 1 and isbilling = 1 and iswin = 1 then winprice/1000 else 0 end ) as adConsumption, " +
//      "(case when iseffective = 1 and isbilling = 1 and iswin = 1 then adpayment/1000 else 0 end ) as adPay " +
//      "from tmp " +
//      ") " +
//      " t" +
//      " group by provincename ,cityname " +
//      "order by adPay desc ").show()


    sc.stop()


  }

}
