package com.utils


import org.apache.spark.broadcast.Broadcast

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object cleanMap {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[1]")

    val sc = new SparkContext(conf)

    val value = sc.textFile( "C:\\Users\\88220\\Desktop\\spark/app_dict.txt" ).filter(_.size>1)
      .map( x => {
          val arr = x.split( "\t" )
          ( arr( 1 ) , arr( 4 ))
      } ).saveAsTextFile("D:\\ideaProject\\sparkTest_1/clean_up")


//    var map :mutable.Map[String,String] = mutable.Map()
//
//    value.map(x=>{
//      map.update(x._1,x._2)
//    })

 //   val broadcast: Broadcast[mutable.Map[String, String]] = sc.broadcast(map)

//    broadcast.value.getOrElse()




  }

}
