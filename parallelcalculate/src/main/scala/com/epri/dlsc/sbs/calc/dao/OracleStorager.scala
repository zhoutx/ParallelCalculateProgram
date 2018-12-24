package com.epri.dlsc.sbs.calc.dao

import com.epri.dlsc.sbs.calc.DataSet.DataSet
import com.epri.dlsc.sbs.calc.service.SettleSession
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import com.epri.dlsc.sbs.calc.DataSet.Field
import scala.collection.mutable
import com.epri.dlsc.sbs.calc.config.Configuration.{pwd, url, user}

object OracleStorager {
  def save(
      dataSetConfig: SettleSession#DataSetConfig,
      dataSetBroadcastVar: Broadcast[mutable.HashMap[String, DataSet]],
      saveData: (String, DataFrame)): Unit = {
    val dataSet = dataSetConfig(saveData._1)
    require(!dataSet.targetTable.isEmpty, s"数据集【${dataSet.name}】结果映射配置错误")
    val insertSql = generateInsertSql(dataSet)
    val updateSql = generateUpdateSql(dataSet)
    saveData._2.repartition(3).foreachPartition(partition => {
      val dataSetBroadcastConf = dataSetBroadcastVar.value(saveData._1)
      val primaryKeyColName = dataSetBroadcastConf.targetTable.primaryKey
      val insertOrUpdateParams = partition.map(row => {
        val insertParam = mutable.ListBuffer[Any]()
        val updateParam = mutable.ListBuffer[Any]()
        val fields = dataSetBroadcastConf.fields
        var updateIdx = 0
        for(field <- fields if !field.targetColumn.isEmpty){
          val rowIdxValue = row.get(row.fieldIndex(field.id))
          val oldId = row.get(row.fieldIndex(Field._OLD_ID_))
          if(oldId == null){
            insertParam += rowIdxValue
          }else{
            val targetColumnName = field.targetColumn
            if(!primaryKeyColName.equals(targetColumnName)) {
              updateParam += rowIdxValue
            }
            if(updateIdx == fields.size){
              updateParam += oldId
            }
          }
          updateIdx += 1
        }
        (insertParam, updateParam)
      })
      val insertParams = insertOrUpdateParams.map(_._1)
      val updateParams = insertOrUpdateParams.map(_._2)
      batchExecuteWidthSql(insertSql, insertParams)
      batchExecuteWidthSql(updateSql, updateParams)
    })
  }

  private def batchExecuteWidthSql(sql: String, params: Iterator[Seq[Any]]): Unit = {
    val connection = java.sql.DriverManager.getConnection(url, user, pwd)
    connection.setAutoCommit(false)
    val statement = connection.prepareStatement(sql)
    try{
      var rowIndx = 0
      for(rowParams <- params){
        for(indx <- rowParams.indices){
          statement.setObject(indx + 1, rowParams(indx))
        }
        statement.addBatch()
        if(rowIndx > 0 && rowIndx%2000 == 0){
          statement.executeBatch()
        }
        rowIndx += 1
      }
      statement.executeBatch()
      connection.commit()
    }catch {
      case e: Exception => {
        if(connection != null) connection.rollback()
        e.printStackTrace()
        throw new RuntimeException(e)
      }
    }finally {
      if(statement != null) statement.close()
      if(connection != null) connection.close()
    }
  }

  private def generateUpdateSql(dataSet: DataSet): String = {
    val primaryKeyColName = dataSet.targetTable.primaryKey
    val saveTableName = dataSet.targetTable.tableName
    val updateSql = new StringBuilder("UPDATE ").append(saveTableName).append(" SET ")
    val fields = dataSet.fields
    for (saveField <- fields if saveField.targetColumn != null) {
      val targetColumnName = saveField.targetColumn
      if (!primaryKeyColName.equals(targetColumnName)) {
        updateSql.append(targetColumnName)
        if (saveField.dataType == Field.DATE) updateSql.append(" = ").append(" TO_DATE(?,'YYYYMMDDHH24MISS'),")
        else updateSql.append(" = ?,")
      }
    }
    updateSql.deleteCharAt(updateSql.length - 1)
    updateSql.append(" WHERE ").append(primaryKeyColName).append(" = ?")
    if (updateSql.isEmpty) null else updateSql.toString
  }
  private def generateInsertSql(dataSet: DataSet): String = {
    val saveTableName = dataSet.targetTable.tableName
    val fields = dataSet.fields
    val insertSql = new StringBuilder("INSERT INTO ").append(saveTableName).append("(")
    for (saveField <- fields if saveField.targetColumn != null) {
      val targetColumnName = saveField.targetColumn
      insertSql.append(targetColumnName).append(",")
    }
    insertSql.deleteCharAt(insertSql.length() - 1).append(") VALUES (")
    for (saveField <- fields if saveField.targetColumn != null) {
      if(saveField.dataType == Field.DATE) {
        insertSql.append(" TO_DATE(?,'YYYYMMDDHH24MISS'),")
      }else {
        insertSql.append( "?,")
      }
    }
    insertSql.deleteCharAt(insertSql.length() - 1).append(")")
    if(insertSql.isEmpty) null else insertSql.toString()
  }
}
