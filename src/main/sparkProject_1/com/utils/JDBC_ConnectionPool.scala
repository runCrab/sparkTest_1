package com.utils

import java.sql.{Connection, DriverManager}
import java.util
import java.util.ResourceBundle

import org.apache.spark.internal.Logging

object JDBC_ConnectionPool extends Logging {

  private val reader = ResourceBundle.getBundle( "properties/version" +
    "_manager_jdbc" )

  //最大连接数
  private val max_connection = reader.getString( "max_connection" )
  //当前连接数
  private val connection_num = reader.getString( "connection_num" )
  private var current_num = 0
  private val pools = new util.LinkedList[Connection]()
  private val driver = reader.getString( "driver" )
  private val url = reader.getString( "url" )
  private val username = reader.getString( "username" )
  private val password = reader.getString( "password" )


  /**
    * 加载驱动
    */
  private def before(): Unit = {
    if (current_num > max_connection.toInt && pools.isEmpty) {
      println( "busyness" )
      Thread.sleep( 2000 )
      before()
    } else {
      Class.forName( driver )
    }
  }

  /**
    * get connection information
    *
    * @return
    */

  private def getConnectionInfo(): Connection = {

    val conn = DriverManager.getConnection( url, username, password )
    conn

  }

  /**
    * init connection pools
    *
    * @return
    */
  private def initConnection(): util.LinkedList[Connection] = {

    //初始化的时候先加锁，防止其他线程同时进行初始化造成冲突
    AnyRef.synchronized( {
      //初始化的时候先对当前连接池进行判断，是否有空余进程可供使用。
      if (pools.isEmpty) {
        before()
        for (i <- 1 to connection_num.toInt) {
          pools.push( getConnectionInfo() )
          current_num += 1
        }
      }
      pools
    } )
  }

  /**
    * 获得一个连接池内的连接
    */
  def getConnection(): Unit ={
    initConnection()
    pools.poll()
  }

  def releaseCon(con:Connection): Unit ={
    pools.push(con)
  }


}
