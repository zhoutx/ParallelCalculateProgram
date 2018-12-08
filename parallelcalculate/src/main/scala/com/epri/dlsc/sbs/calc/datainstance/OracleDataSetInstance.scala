package com.epri.dlsc.sbs.calc.datainstance

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}
import java.util.{Properties, UUID}

import com.epri.dlsc.sbs.calc.DataSet.{DataSet, Field}
import com.epri.dlsc.sbs.calc.config.Configuration
import com.epri.dlsc.sbs.calc.dao.Dao
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable


class OracleDataSetInstance(
     val dataSetId: String,
     val dataRDD: DataFrame)
object OracleDataSourceRDD{
  def apply(
     dataSetId: String,
     dataRDD: DataFrame): OracleDataSetInstance =
    new OracleDataSetInstance(dataSetId, dataRDD)
}

object OracleDataSourceLoader{

  def load(sparkSession: SparkSession, dataSet: DataSet, dataSetBroadcastVar: Broadcast[mutable.HashMap[String, DataSet]]): OracleDataSetInstance = {
    val sql = dataSet.script
    if (sql == null) throw new RuntimeException(s"数据集[${dataSet.name}]没定义数据源脚本语句")
    var executeSql = sql
    if(!dataSet.targetTable.isEmpty){
      var whereCondition: String = ""
      dataSet.constraintFields.foreach(field => {
        if(whereCondition.length > 0){
          whereCondition += "AND"
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
      val count1 = Dao.queryForIntWithSql(s"select count(1) from $sql")
      val count2 = Dao.queryForIntWithSql(s"select count(1) from $executeSql")
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
    val cloumns = originDataFrame.schema.fields
    //将数据统一格式化为字符串形式的数据
    val rddRow = originDataFrame.rdd.mapPartitions(partition => {
      val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
      partition.map(row => {
        val fieldValues = cloumns.map(column => {
          column.dataType.typeName match{
            case "binary" => UUID.randomUUID().toString.replaceAll("-", "")
            case "string" => row.getAs[String](column.name)
            case "timestamp" => {
              val timeValue = row.getAs[java.sql.Timestamp](column.name)
              if(timeValue == null) null else{
                val localDateTime = LocalDateTime.ofInstant(timeValue.toInstant, ZoneId.systemDefault)
                dateTimeFormatter.format(localDateTime)
              }
            }
            case x if x.indexOf("decimal") != -1 =>{
              val decimalValue = row.getAs[java.math.BigDecimal](column.name)
              if(decimalValue == null) null else decimalValue.toString
            }
          }
        })
        Row(fieldValues)
      })
    })
    //数据集列定义
    val newDataFrameSchema = StructType(cloumns.map(cloumn => {
      val fieldName = dataSet.getFieldName(cloumn.name)
      val name = if(fieldName != null) fieldName else cloumn.name
      StructField(name, StringType)
    }))
    //将加载的原始DataFrame转换成按照数据集定义要求的DataFrame
    val dataSetInstanceDataFrame = sparkSession.createDataFrame(rddRow, newDataFrameSchema)

    OracleDataSourceRDD(dataSet.id, dataSetInstanceDataFrame)
  }
}