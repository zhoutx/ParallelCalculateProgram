package com.epri.dlsc.sbs.calc.DataSet

import scala.collection.immutable


/**
  * 数据集
  *
  * @param id 数据集ID
  * @param name 数据集名称
  * @param script 数据源脚本
  * @param fields 字段
  * @param targetTable 目标映射
  */
class DataSet(
    val id: String,
    val name: String,
    val script: String,
    val fields: Seq[Field],
    val targetTable: PersistTable
             ) extends Serializable {
  val constraintFields: Seq[Field] = fields.filter(_.isConstraint)
}
object DataSet {
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
class PersistTable(
  val tableName: String,
  val primaryKey: String)
object PersistTable{
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
class Field(
    val id: String,
    val originId: String,
    val name: String,
    val dataType: Int,
    val isConstraint: Boolean,
    val targetColumn: String = null) extends Serializable {
}
object Field {
  val STRING: Int = 1
  val NUMBER: Int = 2
  val DATE: Int = 3
  def apply(
    id: String,
    originId: String,
    name: String,
    dataType: Int,
    isConstraint: Boolean,
    targetColumn: String = null): Field = new Field(id, originId, name, dataType, isConstraint, targetColumn)
}

class ScriptParams private(
    cParamName: String,
    cParamValue: String){
  private var params = immutable.Map[String, String](cParamName -> cParamValue)
  def addParam(paramName: String, paramValue: String): this.type = {
    params += (paramName -> paramValue)
    this
  }
}
object ScriptParams{
  def addParam(paramName: String, paramValue: String): ScriptParams = new ScriptParams(paramName, paramValue)
}

class SourceTarget(val source: String, val target: String)
object SourceTarget{
  def apply(source: String, target: String): SourceTarget = new SourceTarget(source, target)
}
