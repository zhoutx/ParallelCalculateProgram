package com.epri.dlsc.sbs.calc.config

import com.epri.dlsc.sbs.calc.DataSet.ScriptParamExpression.ScriptParamExpressionParser
import com.epri.dlsc.sbs.calc.DataSet.{DataSet, Field, PersistTable, ScriptParamExpression}
import com.epri.dlsc.sbs.calc.dao.Dao

import scala.collection.mutable
import scala.collection.immutable


object Configuration {
  private val cfg = new java.util.Properties()
  cfg.load(Configuration.getClass.getClassLoader.getResourceAsStream("db-authentication.properties"))
  val url = cfg.getProperty("url")
  val user = cfg.getProperty("user")
  val pwd = cfg.getProperty("password")

  /**
    *
    * @param marketId 场景
    * @param source 数据源标识
    * @param result 结果映射标识
    * @param scriptParams 脚本参数值
    * @param dataSetIDs 数据集IDs
    * @return 符合条件的数据集
    */
  def queryDataSets(
      marketId: String,
      source: String,
      result: String,
      scriptParams: Map[String, String],
      dataSetIDs: Seq[String]): Seq[DataSet] = {
    val dataSetIdString =
      if(dataSetIDs == null || dataSetIDs.size < 1){
        null
      }else if(dataSetIDs.size > 1){
        dataSetIDs.reduce("'" + _ + "'" + "," + "'" + _ + "'")
      }else{
        "'" + dataSetIDs + "'"
      }
    val dataSetIdCondition = if(dataSetIdString == null) "=0" else s"IN ($dataSetIdString)"
    //数据集基本信息
    val baseInfoSql =
      s"""
        |SELECT D.ID, D.DATASET_NAME, S.SQL
        |FROM SE_UPG_DATASET_DEFINE D, SE_UPG_DATASET_SOURCE S
        |WHERE D.IS_DELETE = 0
        |AND D.ID = S.DATASET_ID
        |AND D.MARKET_ID = ?
        |AND S.SOURCE = ? AND D.ID $dataSetIdCondition
      """.stripMargin
    val baseInfoList = Dao.queryForListWithSql(baseInfoSql, Array(marketId, source))
    //数据集字段信息
    val fieldInfoSql =
      s"""
        |SELECT F.DATASET_ID,
        |F.ORIGIN_COL  ORIGIN_ID,
        |F.CUSTOM_COL  ID,
        |F.COL_CAPTION NAME,
        |F.DATA_TYPE,
        |F.IS_CONDITION,
        |T.SAVE_TABLE_NAME,
        |T.PRIMARY_KEY_COL_NAME,
        |T.TARGET_COL_ID
        |FROM SE_UPG_DATASET_FIELD F,
        |  (SELECT R.DATASET_ID,R.SAVE_TABLE_NAME,R.PRIMARY_KEY_COL_NAME,
        |          M.COL_ID,M.TARGET_COL_ID
        |   FROM SE_UPG_DATASET_RESULT R,
        |        SE_UPG_SAVE_MAPPING M
        |   WHERE R.ID = M.RESULT_ID
        |   AND R.RESULT = ?
        |     ) T
        |WHERE F.DATASET_ID = T.DATASET_ID(+)
        |AND F.CUSTOM_COL = T.COL_ID(+)
        |AND F.DATASET_ID $dataSetIdCondition
      """.stripMargin
    val fieldInfoList = Dao.queryForListWithSql(fieldInfoSql, Array(result))

    var fieldMap = mutable.Map[String, mutable.ListBuffer[mutable.Map[String, _]]]()
    fieldInfoList.foreach(x => {
      val dataSetId = x("DATASET_ID").toString
      if(fieldMap.contains(dataSetId)){
        fieldMap(dataSetId) += x
      }else{
        fieldMap += (dataSetId -> mutable.ListBuffer(x))
      }
    })
    baseInfoList.map(info => {
      val dataSetId = info("ID").toString
      val dataSetName = info("DATASET_NAME").toString
      val sql = if(info("SQL") == null){
        null
      }else{
        val clobSql = info("SQL").asInstanceOf[oracle.sql.CLOB]
        clobSql.getSubString(1, clobSql.length().toInt)
      }
      val executeSql = replaceSqlParameters(sql, scriptParams)
      val fields = if(fieldMap.get(dataSetId).isEmpty) List() else fieldMap(dataSetId)
      val targetTableName = if(fields.nonEmpty) fields.head("SAVE_TABLE_NAME").toString else null
      val primaryKey = if(fields.nonEmpty) fields.head("PRIMARY_KEY_COL_NAME").toString else null
      val fieldList = fields.map(field => {
        val id = field("ID").toString
        val originId = field("ORIGIN_ID").toString
        val name = field("NAME").toString
        val dataType = if(field("DATA_TYPE") == null) 0 else field("DATA_TYPE").toString.toInt
        val isConstraint: Boolean = if(field("IS_CONDITION") == null) false else if(1 == field("IS_CONDITION").toString.toInt) true else false
        val targetColumn = if(field("TARGET_COL_ID") == null) null else field("TARGET_COL_ID").toString
        Field(id, originId, name, dataType, isConstraint, targetColumn)
      })
      DataSet(dataSetId, dataSetName, executeSql, fieldList, PersistTable(targetTableName, primaryKey))
    })
  }
  def replaceSqlParameters(sql: String, scriptParams: Map[String, String]): String = {
    var returnSql: String = sql
    val paramExpressions = ScriptParamExpressionParser.getExpressions(sql)
    if(paramExpressions.nonEmpty && (scriptParams == null || scriptParams.isEmpty)) throw new RuntimeException("入参[scriptParams]不可为空！")
    paramExpressions.foreach(expression => {
      val condtionValue = if(scriptParams.get(expression.paramName).isEmpty) null else scriptParams(expression.paramName)
      if (condtionValue == null) throw new RuntimeException("入参[scriptParams]缺少条件:" + expression.paramName + "！")
      if (expression.paramType == ScriptParamExpression.VARIABLE) returnSql = returnSql.replace(expression.paramExpression, "'" + condtionValue + "'")
      else if (expression.paramType == ScriptParamExpression.STATIC) returnSql = returnSql.replace(expression.paramExpression, condtionValue)
    })
    returnSql
  }
}
