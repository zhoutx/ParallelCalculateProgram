package com.epri.dlsc.sbs.calc.service

import com.epri.dlsc.sbs.calc.DataSet.DataSetExpression
import com.epri.dlsc.sbs.calc.formula.{Formula, FormulaItem}

import scala.collection.immutable


/**
  * 1、加载数据源、公式、算法基本信息，并共享
  * 2、分析公式，得出需要按主体加载的数据和需要共享的数据
  * 3、按主体加载的数据按主体数据分区，并按主体排序或分组
  * 4、计算按分区执行，每个分区建立计算缓存
  */
object SettleCalculateTask{

  def main(args: Array[String]): Unit = {

  }

  /**
    *计算任务提交
    * @param params
    * @return
    */
  def apply(
     marketId: String,
     sourceTarget: SourceTarget,
     scriptParams: ScriptParams,
     formulaIds: Array[String]): SettleCalculateTask
   = new SettleCalculateTask(marketId, sourceTarget, scriptParams, formulaIds)

}
class SettleCalculateTask(
     marketId: String,
     sourceTarget: SourceTarget,
     scriptParams: ScriptParams,
     formulaIds: Array[String]){
  def submit(): Boolean = {

    true
  }
}



class CalcDataSourceMatcher(
     formulaInfo: Formula,
     leftDataSet: OracleDataSource,
     rightDataSets: Seq[OracleDataSource],
     dataSetExp: Seq[DataSetExpression]){
  //匹配缓存
  val matchCache
  //执行数据配对
  def executeMatch(): xxx

}

class FormulaExecutor(formula: FormulaItem, xxx){
  //执行计算
  def execute(): OracleDataSource = {

  }
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