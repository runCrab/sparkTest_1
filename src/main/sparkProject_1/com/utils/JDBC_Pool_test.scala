package com.utils

import java.sql.{Connection, DriverManager}
import java.util

class JDBC_Pool_test {
  private val max = 10
  private val ConnectionNum = 10
  private var conNum = 0
  private  val pool = new util.LinkedList[Connection]


  private def getDriver(): Unit ={
    if(conNum < max && pool.isEmpty){
      Class.forName("com.mysql.Driver")
    }else if (conNum >= max && pool.isEmpty){
      println("当前无可用Connection")
      getDriver()
    }
  }
  def getConn ():Connection = {

    if (pool.isEmpty){
      getDriver()
      for (i <- 1 to ConnectionNum){
        val conn = DriverManager.getConnection("jdbc:mysql://hadoop01:3306/test",
          "root","123456")
        pool.push(conn)
        conNum +=1
      }
    }
    val coon = pool.pop()
    coon
  }
  def returnConn(conn: Connection): Unit ={
    pool.push(conn)
  }

}
