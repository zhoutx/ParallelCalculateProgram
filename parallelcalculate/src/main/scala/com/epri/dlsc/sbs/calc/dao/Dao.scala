package com.epri.dlsc.sbs.calc.dao

import java.sql.{Connection, ResultSet}

import com.epri.dlsc.sbs.calc.config.Configuration.{pwd, url, user}

import scala.collection.mutable

object Dao {

  private var connection = java.sql.DriverManager.getConnection(url, user, pwd)

  def getConnection: Connection = {
    if (connection.isClosed) {
      connection = java.sql.DriverManager.getConnection(url, user, pwd)
    }
    connection
  }

  def queryForListWithSql(
       sql: String,
       params: Array[AnyRef] = null): Seq[mutable.Map[String, AnyRef]] = {
    val statement = getConnection.prepareStatement(sql)
    if (params != null) {
      var index = 1
      for (param <- params) {
        statement.setObject(index, param)
        index += 1
      }
    }
    var queryResultSet: ResultSet = null
    try{
      queryResultSet = statement.executeQuery()
      val metaData = queryResultSet.getMetaData
      val columns = for (index <- 1 to metaData.getColumnCount) yield metaData.getColumnName(index)
      val resultList = mutable.ListBuffer[mutable.HashMap[String, AnyRef]]()
      while (queryResultSet.next()) {
        var data = mutable.HashMap[String, AnyRef]()
        var i = 1
        columns.foreach(column => {
          data += (column -> queryResultSet.getObject(i))
          i += 1
        })
        resultList += data
      }
      resultList
    }finally{
      if(queryResultSet != null) queryResultSet.close()
      if(statement != null) statement.close()
    }
  }

  def queryForStringWithSql(
      sql: String,
      params: Array[AnyRef] = null): String = {
    val statement = getConnection.prepareStatement(sql)
    if (params != null) {
      var index = 1
      for (param <- params) {
        statement.setObject(index, param)
        index += 1
      }
    }
    var queryResultSet: ResultSet = null
    try{
      queryResultSet = statement.executeQuery()
      queryResultSet.next()
      val value = queryResultSet.getObject(1)
      if (value != null) {
        if (value.isInstanceOf[oracle.sql.CLOB]) {
          val clobValue = value.asInstanceOf[oracle.sql.CLOB]
          clobValue.getSubString(1, clobValue.length().toInt)
        }else{
          value.asInstanceOf[String]
        }
      }else null
    }finally {
      if(queryResultSet != null) queryResultSet.close()
      if(statement != null) statement.close()
    }


  }

  def queryForIntWithSql(
      sql: String,
      params: Array[AnyRef] = null): Int = {
    val statement = getConnection.prepareStatement(sql)
    if (params != null) {
      var index = 1
      for (param <- params) {
        statement.setObject(index, param)
        index += 1
      }
    }
    var queryResultSet: ResultSet = null
    try{
      queryResultSet = statement.executeQuery()
      queryResultSet.next()
      queryResultSet.getInt(1)
    }finally{
      if(queryResultSet != null) queryResultSet.close()
      if(statement != null) statement.close()
    }
  }

}
