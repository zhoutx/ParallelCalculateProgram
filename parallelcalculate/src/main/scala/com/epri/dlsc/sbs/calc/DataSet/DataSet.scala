package com.epri.dlsc.sbs.calc.DataSet

import scala.collection.mutable


/**
  * 数据集
  *
  * @param id 数据集ID
  * @param name 数据集名称
  * @param script 数据源脚本
  * @param fields 字段
  * @param targetTable 目标映射
  */
private[calc] class DataSet(
    val id: String,
    val name: String,
    val script: String,
    val fields: Seq[Field],
    val targetTable: PersistTable
             ) extends Serializable {
  val constraintFields: Seq[Field] = fields.filter(_.isConstraint)
  val sqlColumnName_Field_Map: mutable.HashMap[String, Field] = mutable.HashMap[String, Field]()
  def getField(sqlColumnName: String): Field = {
    if(sqlColumnName_Field_Map.contains(sqlColumnName)){
      sqlColumnName_Field_Map(sqlColumnName)
    }else{
      val field = fields.filter(field => field.originId.equals(sqlColumnName))
      if(field != null && field.size == 1){
        val requireField = field.head
        sqlColumnName_Field_Map += (sqlColumnName -> requireField)
        requireField
      }else{
        null
      }
    }
  }
}
private[calc] object DataSet {
  def apply(
    id: String,
    name: String,
    script: String,
    fields: Seq[Field],
    targetTable: PersistTable): DataSet = new DataSet(id, name, script, fields, targetTable)
}

/**
  * 目标
  * @param tableName  目标表名
  * @param primaryKey 目标表主键
  */
private[calc] class PersistTable(
  val tableName: String,
  val primaryKey: String) extends Serializable{
  def isEmpty: Boolean = tableName == null || primaryKey == null
}
private[calc] object PersistTable{
  def apply(
    tableName: String,
    primaryKey: String): PersistTable = new PersistTable(tableName, primaryKey)
}

/**
  * 数据集字段
  * @param id 字段ID
  * @param originId 原始字段
  * @param name 字段名称
  * @param dataType 数据类型
  * @param isConstraint 是否约束字段
  * @param targetColumn 所映射的目标表列名
  */
private[calc] class Field(
    val id: String,
    val originId: String,
    val name: String,
    val dataType: Int,
    val isConstraint: Boolean,
    val targetColumn: String = null) extends Serializable
private[calc] object Field {
  val STRING: Int = 1
  val NUMBER: Int = 2
  val DATE: Int = 3
  val _OLD_ID_ : String = "OLD_ID"
  def apply(
    id: String,
    originId: String,
    name: String,
    dataType: Int,
    isConstraint: Boolean,
    targetColumn: String = null): Field = new Field(id, originId, name, dataType, isConstraint, targetColumn)
}
