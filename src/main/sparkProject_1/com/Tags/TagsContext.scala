

import com.Tags.{BusinessTag, BusinessTag_1, TagApp, TagLoc, TagTerminal, keyWord}
import com.utils.{JDBC_Pool_test, TagUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

import scala.collection.immutable

/**
  * 上下文标签
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    //    if (args.length != 4) {
    //      println( "目录不匹配，退出程序" )
    //      sys.exit()
    //    }
    //val Array( inputPath, outputPath ) = args

    val inputPath1 = "D:\\ideaProject\\sparkTest_1\\parquet_out"
//    val inputPath2 = "C:\\Users\\88220\\Desktop\\spark/stopwords.txt"

    // 创建上下文
    val conf = new SparkConf().setAppName( this.getClass.getName ).setMaster( "local[*]" )
    val sc = new SparkContext( conf )
    val sQLContext = new SQLContext( sc )
    // 读取数据
    val df = sQLContext.read.parquet( inputPath1 )
    // 过滤符合Id的数据
//    val braodcast = sc.textFile( inputPath2 ).map( (_, 0) ).collectAsMap()

//    val bd: Broadcast[collection.Map[String, Int]] = sc.broadcast( braodcast )

    df.show(1)
    import sQLContext.implicits._

//    //广告位类型
//    val ad = df.filter( TagUtils.OneUserId )
//      // 接下来所有的标签都在内部实现
//      .map( row => {
//      // 取出用户Id
//      val userId = TagUtils.getOneUserId( row )
//      // 接下来通过row数据 打上 所有标签（按照需求）
//      val adList = TagsAd.makeTags( row )
//      ((userId), adList)
//    } ).rdd.groupByKey().foreach( println )
//
//    //App 名称
//    val app = df.filter( TagUtils.OneUserId )
//      .map( row => {
//        val userId = TagUtils.getOneUserId( row )
//        val appname = TagApp.makeTags( row )
//        ((userId), appname)
//      } ).rdd.groupByKey()
//
//    //channel
//    val channel = df.filter( TagUtils.OneUserId )
//      .map( row => {
//        val userId = TagUtils.getOneUserId( row )
//        val appname = TagChannel.makeTags( row )
//        ((userId), appname)
//      } ).rdd.groupByKey()
//
//
//    //terminal
//    val terminal: RDD[(String, Iterable[List[(String, Int)]])] = df.filter( TagUtils.OneUserId )
//      .map( row => {
//        val userId = TagUtils.getOneUserId( row )
//        val appname = TagTerminal.makeTags( row )
//        ((userId), appname)
//      } ).rdd.groupByKey()
//
//    //keyword
//    val keyword= df.filter( TagUtils.OneUserId )
//      .map( x => {
//        val userId = TagUtils.getOneUserId( x )
//        val appname = keyWord.makeTags( x,bd )
//        ((userId), appname)
//      } ).rdd.groupByKey().take(10).foreach(println)
//
////println("*"*50)
//    //pro_city
//    val proAndcity = df.filter( TagUtils.OneUserId )
//      .map( row => {
//        val userId = TagUtils.getOneUserId( row )
//        val proandcity = TagLoc.makeTags( row )
//        ((userId), proandcity)
//      } ).rdd.groupByKey().take(10).foreach(println)

    df.filter( TagUtils.OneUserId )
        .map(row =>{
          //拿到userid
          val userId = TagUtils.getOneUserId(row)
          //获取标签信息
          val busInfo: Seq[(String, Int)] = BusinessTag_1.makeTags(row)
          (userId,busInfo)
        }).rdd.groupByKey().foreach(println)
//    println( keyword )
//    println( proAndcity )


  }
}
