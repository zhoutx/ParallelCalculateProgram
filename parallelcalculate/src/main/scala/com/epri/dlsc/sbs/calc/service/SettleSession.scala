package com.epri.dlsc.sbs.calc.service

import com.epri.dlsc.sbs.calc.DataSet.{DataSet, DataSetExpression}
import com.epri.dlsc.sbs.calc.config.Configuration
import com.epri.dlsc.sbs.calc.datainstance.OracleDataSetInstance
import com.epri.dlsc.sbs.calc.formula.{CalcuateItemExpression, Formula, FormulaItem}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.{immutable, mutable}


/**
  * 1、加载数据源、公式、算法基本信息，并共享
  * 2、分析公式，得出需要按主体加载的数据和需要共享的数据
  * 3、按主体加载的数据按主体数据分区，并按主体排序或分组
  * 4、计算按分区执行，每个分区建立计算缓存
  */
object SettleSession{

  def getOrCreate: SettleSession = synchronized{
    val session = activeThreadSession.get()
    if(session ne null) session else newCreate
  }

  def newCreate: SettleSession = {
    val session = new SettleSession()
    activeThreadSession.remove()
    activeThreadSession.set(session)
    session
  }

  private val activeThreadSession = new ThreadLocal[SettleSession]

}
class SettleSession private{

  private def formulaAnalyze(formula: Formula): Seq[(CalcuateItemExpression, Seq[DataSetExpression], String)] = {

  }

  private def execute(preparedFormula: Seq[Seq[(CalcuateItemExpression, Seq[DataSetExpression], String)]]): Boolean = {
    preparedFormula.foreach(formula => {
      val scopeDataCache = mutable.HashMap[String, RDD[(String, DataFrame)]]()
      formula.foreach(item =>{
        val calcItem: CalcuateItemExpression = item._1
        val matchedDataSets: Seq[DataSetExpression] = item._2
        val preparedFormulaContent = item._3
        val leftData = dataSetInstanceCollectionMap.getOrElse(calcItem.dataSetId, null)
        require(leftData != null,
          s"待计算的数据集【${calcItem.dataSetId}】加载失败")
        if(matchedDataSets.size < 1){
          leftData
        }else{

        }

      })
    })
    true
  }

  /**
    * 加载数据集数据实体
    * @param preparedFormula
    */
  private def dataSetInstanceLoad(preparedFormula: Seq[(CalcuateItemExpression, Seq[DataSetExpression], String)]): Unit={
    val usedDataSetIds = mutable.Set[String]()
    preparedFormula.foreach(x => {
      usedDataSetIds += x._1.dataSetId
      x._2.foreach(y => {
        usedDataSetIds += y.dataSetId
      })
    })
    Configuration.queryDataSets(config.marketId, config.sourceTarget, config.scriptParams, usedDataSetIds.toList)
      .foreach(dataSet => {
        dataSetConfigCollectionMap += (dataSet.id -> dataSet)
      })
    //将数据集基本信息广播
    dataSetBroadcastVar = sparkSession.sparkContext.broadcast[mutable.HashMap[String, DataSet]](dataSetConfigCollectionMap)

  }

  /**
    * 计算任务提交
    * @return
    */
  def submit(): Unit = {
    val formulas = Configuration.queryFormuals(config.marketId, config.formulaIds)
    val preparedFormula = formulas.map(formulaAnalyze)
    dataSetInstanceLoad(preparedFormula.flatMap(x => x.toList))
    execute(preparedFormula)
    sparkSession.close()
  }

  def config(
      marketId: String,
      sourceTarget: SourceTarget,
      scriptParams: ScriptParams,
      formulaIds: Array[String]): SettleSession
  = {
    config = new Config(marketId, sourceTarget, scriptParams, formulaIds)
    this
  }

  private class Config(
     val marketId: String,
     val sourceTarget: SourceTarget,
     val scriptParams: ScriptParams,
     val formulaIds: Seq[String])

  private var config: Config = _
  private val sparkSession: SparkSession = SparkSession.builder().appName("Settle System").master("local[10]").getOrCreate()
  private val dataSetInstanceCollectionMap = mutable.HashMap[String, OracleDataSetInstance]()
  private val dataSetConfigCollectionMap = mutable.HashMap[String, DataSet]()
  private var dataSetBroadcastVar: Broadcast[mutable.HashMap[String, DataSet]] = _
  private val formulaConfigCollectionMap = mutable.HashMap[String, Formula]()
}



//class CalcDataSourceMatcher(
//     formulaInfo: Formula,
//     leftDataSet: OracleDataSource,
//     rightDataSets: Seq[OracleDataSource],
//     dataSetExp: Seq[DataSetExpression]){
//  //匹配缓存
//  val matchCache
//  //执行数据配对
//  def executeMatch(): xxx
//
//}
//
//class FormulaExecutor(formula: FormulaItem, xxx){
//  //执行计算
//  def execute(): OracleDataSource = {
//
//  }
//}

class ScriptParams private(
    cParamName: String,
    cParamValue: String){
  private var params = immutable.Map[String, String](cParamName -> cParamValue)
  def addParam(paramName: String, paramValue: String): this.type = {
    params += (paramName -> paramValue)
    this
  }
  private[calc] def toMap: Map[String, String] = params
}
object ScriptParams{
  def addParam(paramName: String, paramValue: String): ScriptParams = new ScriptParams(paramName, paramValue)
}

class SourceTarget(val source: String, val target: String)
object SourceTarget{
  def apply(source: String, target: String): SourceTarget = new SourceTarget(source, target)
}