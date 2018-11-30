package com.epri.dlsc.sbs.calc.service

import com.epri.dlsc.sbs.calc.DataSet.DataSetExpression
import com.epri.dlsc.sbs.calc.datasource.OracleDataSource
import com.epri.dlsc.sbs.calc.formula.{Formula, FormulaItem}


/**
  * 1、加载数据源、公式、算法基本信息，并共享
  * 2、分析公式，得出需要按主体加载的数据和需要共享的数据
  * 3、按主体加载的数据按主体数据分区，并按主体排序或分组
  * 4、计算按分区执行，每个分区建立计算缓存
  */
object SparkCalculateService{

  def main(args: Array[String]): Unit = {

  }





  /**
    *计算任务提交
    * @param params
    * @return
    */
  def submit():Boolean = {

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

