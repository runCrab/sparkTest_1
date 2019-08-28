package my

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import project_01.Utils2Type


object day_01 {

  def main(args: Array[String]): Unit = {

    System.setProperty( "hadoop.home.dir", "D:\\Hadoop\\hadoop-2.7.6" )

    //    if(args.length!= 2){
    //
    //      println("输入的参数不正确，退出程序")
    //
    //      sys.exit()
    //    }
    //用一个数组来接收输入的args

    // val Array(inputpath ,outputpath) = args

    //配置spark环境信息
    val conf = new SparkConf().setAppName( this.getClass.getName )
      .setMaster( "local[1]" )
      .set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
    val sc = new SparkContext( conf )
    //val ss = SparkSession.builder().config(conf).getOrCreate()
    val ssc = SparkSession.builder().config( conf ).getOrCreate()

    val SqlContext = new SQLContext( sc )
    SqlContext.setConf( "spark.sql.parquet.compression.codec", "snappy" )
    val inputpath = "C://Users//88220//Desktop//spark/2016-10-01_06_p1_invalid.1475274123982.log"
    val line = sc.textFile( inputpath )

    val word: RDD[struct] = line.map( x => {
      x.split( ",", x.length )
    } ).filter( _.length >= 85 ).map( arr => {
      struct(
        arr( 0 ),
        Utils2Type.toInt( arr( 1 ) ),
        Utils2Type.toInt( arr( 2 ) ),
        Utils2Type.toInt( arr( 3 ) ),
        Utils2Type.toInt( arr( 4 ) ),
        arr( 5 ),
        arr( 6 ),
        Utils2Type.toInt( arr( 7 ) ),
        Utils2Type.toInt( arr( 8 ) ),
        Utils2Type.toDouble( arr( 9 ) ),
        Utils2Type.toDouble( arr( 10 ) ),
        arr( 11 ),
        arr( 12 ),
        arr( 13 ),
        arr( 14 ),
        arr( 15 ),
        arr( 16 ),
        Utils2Type.toInt( arr( 17 ) ),
        arr( 18 ),
        arr( 19 ),
        Utils2Type.toInt( arr( 20 ) ),
        Utils2Type.toInt( arr( 21 ) ),
        arr( 22 ),
        arr( 23 ),
        arr( 24 ),
        arr( 25 ),
        Utils2Type.toInt( arr( 26 ) ),
        arr( 27 ),
        Utils2Type.toInt( arr( 28 ) ),
        arr( 29 ),
        Utils2Type.toInt( arr( 30 ) ),
        Utils2Type.toInt( arr( 31 ) ),
        Utils2Type.toInt( arr( 32 ) ),
        arr( 33 ),
        Utils2Type.toInt( arr( 34 ) ),
        Utils2Type.toInt( arr( 35 ) ),
        Utils2Type.toInt( arr( 36 ) ),
        arr( 37 ),
        Utils2Type.toInt( arr( 38 ) ),
        Utils2Type.toInt( arr( 39 ) ),
        Utils2Type.toDouble( arr( 40 ) ),
        Utils2Type.toDouble( arr( 41 ) ),
        Utils2Type.toInt( arr( 42 ) ),
        arr( 43 ),
        Utils2Type.toDouble( arr( 44 ) ),
        Utils2Type.toDouble( arr( 45 ) ),
        arr( 46 ),
        arr( 47 ),
        arr( 48 ),
        arr( 49 ),
        arr( 50 ),
        arr( 51 ),
        arr( 52 ),
        arr( 53 ),
        arr( 54 ),
        arr( 55 ),
        arr( 56 ),
        Utils2Type.toInt( arr( 57 ) ),
        Utils2Type.toDouble( arr( 58 ) ),
        Utils2Type.toInt( arr( 59 ) ),
        Utils2Type.toInt( arr( 60 ) ),
        arr( 61 ),
        arr( 62 ),
        arr( 63 ),
        arr( 64 ),
        arr( 65 ),
        arr( 66 ),
        arr( 67 ),
        arr( 68 ),
        arr( 69 ),
        arr( 70 ),
        arr( 71 ),
        arr( 72 ),
        Utils2Type.toInt( arr( 73 ) ),
        Utils2Type.toDouble( arr( 74 ) ),
        Utils2Type.toDouble( arr( 75 ) ),
        Utils2Type.toDouble( arr( 76 ) ),
        Utils2Type.toDouble( arr( 77 ) ),
        Utils2Type.toDouble( arr( 78 ) ),
        arr( 79 ),
        arr( 80 ),
        arr( 81 ),
        arr( 82 ),
        arr( 83 ),
        Utils2Type.toInt( arr( 84 ) ) )
    } )


    val word1 = line.map( x => {
      x.split( ",", x.length )
    } ).filter( _.length >= 85 ).map( arr => {
      Row(
        arr( 0 ),
        Utils2Type.toInt( arr( 1 ) ),
        Utils2Type.toInt( arr( 2 ) ),
        Utils2Type.toInt( arr( 3 ) ),
        Utils2Type.toInt( arr( 4 ) ),
        arr( 5 ),
        arr( 6 ),
        Utils2Type.toInt( arr( 7 ) ),
        Utils2Type.toInt( arr( 8 ) ),
        Utils2Type.toDouble( arr( 9 ) ),
        Utils2Type.toDouble( arr( 10 ) ),
        arr( 11 ),
        arr( 12 ),
        arr( 13 ),
        arr( 14 ),
        arr( 15 ),
        arr( 16 ),
        Utils2Type.toInt( arr( 17 ) ),
        arr( 18 ),
        arr( 19 ),
        Utils2Type.toInt( arr( 20 ) ),
        Utils2Type.toInt( arr( 21 ) ),
        arr( 22 ),
        arr( 23 ),
        arr( 24 ),
        arr( 25 ),
        Utils2Type.toInt( arr( 26 ) ),
        arr( 27 ),
        Utils2Type.toInt( arr( 28 ) ),
        arr( 29 ),
        Utils2Type.toInt( arr( 30 ) ),
        Utils2Type.toInt( arr( 31 ) ),
        Utils2Type.toInt( arr( 32 ) ),
        arr( 33 ),
        Utils2Type.toInt( arr( 34 ) ),
        Utils2Type.toInt( arr( 35 ) ),
        Utils2Type.toInt( arr( 36 ) ),
        arr( 37 ),
        Utils2Type.toInt( arr( 38 ) ),
        Utils2Type.toInt( arr( 39 ) ),
        Utils2Type.toDouble( arr( 40 ) ),
        Utils2Type.toDouble( arr( 41 ) ),
        Utils2Type.toInt( arr( 42 ) ),
        arr( 43 ),
        Utils2Type.toDouble( arr( 44 ) ),
        Utils2Type.toDouble( arr( 45 ) ),
        arr( 46 ),
        arr( 47 ),
        arr( 48 ),
        arr( 49 ),
        arr( 50 ),
        arr( 51 ),
        arr( 52 ),
        arr( 53 ),
        arr( 54 ),
        arr( 55 ),
        arr( 56 ),
        Utils2Type.toInt( arr( 57 ) ),
        Utils2Type.toDouble( arr( 58 ) ),
        Utils2Type.toInt( arr( 59 ) ),
        Utils2Type.toInt( arr( 60 ) ),
        arr( 61 ),
        arr( 62 ),
        arr( 63 ),
        arr( 64 ),
        arr( 65 ),
        arr( 66 ),
        arr( 67 ),
        arr( 68 ),
        arr( 69 ),
        arr( 70 ),
        arr( 71 ),
        arr( 72 ),
        Utils2Type.toInt( arr( 73 ) ),
        Utils2Type.toDouble( arr( 74 ) ),
        Utils2Type.toDouble( arr( 75 ) ),
        Utils2Type.toDouble( arr( 76 ) ),
        Utils2Type.toDouble( arr( 77 ) ),
        Utils2Type.toDouble( arr( 78 ) ),
        arr( 79 ),
        arr( 80 ),
        arr( 81 ),
        arr( 82 ),
        arr( 83 ),
        Utils2Type.toInt( arr( 84 ) ) )
    } )


    //通过structtype 方式转换成DF
    val df1: DataFrame = SqlContext.createDataFrame( word1, SchemaUtils.structtype )

    //保存成parquet文件写出
    df1.write.parquet( "D:\\ideaProject\\sparkTest_1/parquet_out" )


    import ssc.implicits._
    val df: DataFrame = word.toDF()
    df.createOrReplaceTempView( "tmp" )

    //sparkSql方式转换成json
    SqlContext.sql( "select count(1) as ct ,provincename,cityname from tmp group by provincename,cityname order by ct desc" ).write
      .json( "D:\\ideaProject\\sparkTest_1/sql_json_res1" )

    val provinceAndCityInfo = word.map( x => {
      ((x.provincename, x.cityname), 1)
    } )
    val res: RDD[((String, String), Int)] = provinceAndCityInfo.reduceByKey( _ + _ )

    res.sortBy( _._2 ).map( x => {

      (x._2, x._1._1, x._1._2)
    } ).toDF( "ct", "provincename", "cityname" )
      //json格式写出到本地
      .write.json( s"D:\\ideaProject\\sparkTest_1/core_json_res1" )


  }
}
  case class struct(
                     var sessionid: String,
                     var advertisersid: Int,
                     var adorderid: Int,
                     var adcreativeid: Int,
                     var adplatformproviderid: Int,
                     var sdkversion: String,
                     var adplatformkey: String,
                     var putinmodel: Int,
                     var requestmode: Int,
                     var adprice: Double,
                     var adppprice: Double,
                     var requestdate: String,
                     var ip: String,
                     var appid: String,
                     var appname: String,
                     var uuid: String,
                     var device: String,
                     var client: Int,
                     var osversion: String,
                     var density: String,
                     var pw: Int,
                     var ph: Int,
                     var longtype: String,
                     var lat: String,
                     var provincename: String,
                     var cityname: String,
                     var ispid: Int,
                     var ispname: String,
                     var networkmannerid: Int,
                     var networkmannername: String,
                     var iseffective: Int,
                     var isbilling: Int,
                     var adspace: Int,
                     var adspacename: String,
                     var devicedevicetype: Int,
                     var processnode: Int,
                     var app: Int,
                     var district: String,
                     var paymode: Int,
                     var isbid: Int,
                     var bidprice: Double,
                     var winprice: Double,
                     var iswin: Int,
                     var cur: String,
                     var rate: Double,
                     var cnywinprice: Double,
                     var imei: String,
                     var mac: String,
                     var idfa: String,
                     var openudid: String,
                     var androidid: String,
                     var rtbprovince: String,
                     var rtbcity: String,
                     var rtbdistrict: String,
                     var rtbstreet: String,
                     var storeurl: String,
                     var realip: String,
                     var isqualityapp: Int,
                     var bidfloor: Double,
                     var aw: Int,
                     var ah: Int,
                     var imeimd5: String,
                     var macmd5: String,
                     var idfamd5: String,
                     var openudidmd5: String,
                     var androididmd5: String,
                     var imeisha1: String,
                     var macsha1: String,
                     var idfasha1: String,
                     var openudidsha1: String,
                     var androididsha1: String,
                     var uuidunknow: String,
                     var userid: String,
                     var iptype: Int,
                     var initbidprice: Double,
                     var adpayment: Double,
                     var agentrate: Double,
                     var lomarkrate: Double,
                     var adxrate: Double,
                     var title: String,
                     var keywords: String,
                     var tagid: String,
                     var callbackdate: String,
                     var channelid: String,
                     var media: Int
                   )
