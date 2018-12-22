package com.epri.dlsc.sbs.calc.datainstance

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.{Properties, UUID}

import com.epri.dlsc.sbs.calc.DataSet.{DataSet, Field}
import com.epri.dlsc.sbs.calc.config.Configuration
import com.epri.dlsc.sbs.calc.dao.Dao
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable


private[calc] object OracleDataSet{

  def getInstance(
        sparkSession: SparkSession,
        dataSetBroadcastVar: Broadcast[mutable.HashMap[String, DataSet]],
        dataSet: DataSet): DataFrame = {
    val sql = dataSet.script
    if (sql == null) throw new RuntimeException(s"数据集[${dataSet.name}]没定义数据源脚本语句")
    var executeSql = sql
    if(!dataSet.targetTable.isEmpty){
      var whereCondition: String = ""
      dataSet.constraintFields.foreach(field => {
        if(whereCondition.length > 0){
          whereCondition += " AND "
        }
        whereCondition += s"V.${field.originId}=R.${field.targetColumn}(+)"
      })
      if(whereCondition.length > 0){
        executeSql =
          s"""
             |SELECT R.${dataSet.targetTable.primaryKey} ${Field._OLD_ID_},
             |V.* FROM($sql) V,${dataSet.targetTable.tableName} R
             |WHERE $whereCondition
           """.stripMargin
      }
      //数据源与目标表关系合法性验证
      val count1 = Dao.queryForIntWithSql(s"select count(1) from ($sql)")
      val count2 = Dao.queryForIntWithSql(s"select count(1) from ($executeSql)")
      if(count1 != count2){
        throw new RuntimeException(s"数据集[${dataSet.name}]约束条件和映射关系不足以使得存储表中的数据保证唯一性")
      }
    }

    val dbConn = new Properties()
    dbConn.setProperty("user", Configuration.user)
    dbConn.setProperty("password", Configuration.pwd)
    val originDataFrame = sparkSession.read.jdbc(
      Configuration.url,
      s"($executeSql)",
      dbConn)
    //加载的数据列信息
    val dataSetId = dataSet.id
    val cloumns = originDataFrame.schema.fields
    //将数据统一格式化为字符串形式的数据
    val rddRows = originDataFrame.rdd.mapPartitions(partition => {
      val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
      partition.map(row => {
        val fieldValues = cloumns.map(column => {
          val dataSetField = dataSetBroadcastVar.value(dataSetId).getField(column.name)
          val dataType = if(dataSetField == null) 1 else dataSetField.dataType
          dataType match {
            case 1 => {
              if ("binary".equals(column.dataType.typeName)){
                UUID.randomUUID().toString.replaceAll("-", "")
              }else if(column.dataType.typeName.indexOf("decimal") != -1){
                val decimal = row.getDecimal(row.fieldIndex(column.name))
                if (decimal == null)
                  null
                else
                  decimal.toString
              }else{
                row.getAs[String](column.name)
              }
            }
            case 2 => {
              if ("string".equals(column.dataType.typeName)) {
                val stringValue = row.getAs[String](column.name)
                if (stringValue == null)
                  0D
                else
                  java.lang.Double.valueOf(stringValue)
              } else {
                val decimal = row.getDecimal(row.fieldIndex(column.name))
                if (decimal == null)
                  0D
                else
                  decimal.doubleValue()
              }
            }
            case 3 => {
              val timeValue = row.getAs[java.sql.Timestamp](column.name)
              if (timeValue == null)
                null
              else {
                val localDateTime = LocalDateTime.ofInstant(timeValue.toInstant, ZoneId.systemDefault)
                dateTimeFormatter.format(localDateTime)
              }
            }
            case _ => {
              row.get(row.fieldIndex(column.name)).toString
            }
          }
        })
        val listValue = UUID.randomUUID().toString :: fieldValues.toList
        Row(listValue: _*)
      })
    })

    //数据集列定义
    //添加ROWID
    val newDataFrameSchema = StructType(StructField("ROWID", StringType) :: cloumns.map(column => {
      val field = dataSet.getField(column.name)
      val name = if(field != null) field.id else column.name
      if(field != null && field.dataType == 2){
        StructField(name, DoubleType)
      } else{
        StructField(name, StringType)
      }
    }).toList)

    //将加载的原始DataFrame转换成按照数据集定义要求的DataFrame
    val dataSetInstanceDataFrame = sparkSession.createDataFrame(rddRows, newDataFrameSchema)
    dataSetInstanceDataFrame.cache()

  }
}