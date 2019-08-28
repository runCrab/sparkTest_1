package com.utils

import java.sql.{CallableStatement, PreparedStatement}

import com.mysql.jdbc.Connection

class JDBC_Utils {

  private var conn:Connection = null
  private var preparedStatement : PreparedStatement = null
  private var callableStatement:CallableStatement = null


}
