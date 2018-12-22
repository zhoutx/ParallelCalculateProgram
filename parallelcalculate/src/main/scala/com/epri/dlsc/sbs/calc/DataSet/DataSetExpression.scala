package com.epri.dlsc.sbs.calc.DataSet

import java.util.regex.Pattern

import scala.collection.mutable

/**
  * 数据集表达式
  * @param expression 完整表达式
  * @param dataSetId 数据集Id
  * @param valueField 值
  * @param pwhere 匹配项
  * @param pfilter  过滤项
  *
  *
  *
  *
  */
private[calc] class DataSetExpression(
                         val expression: String,
                         val dataSetId: String,
                         val valueField: String,
                         val where: Seq[String],
                         val filter:Map[String, String] = Map[String, String]()
                       ) {
  private var MATCHID: String = _
  val MATCH_IDENTIFIER: String = if(MATCHID == null){
    MATCHID = dataSetId + "_" +where.sorted.mkString("_")
    if(filter.nonEmpty){
      MATCHID += filter.keySet.toList.sorted.map(key => key+"="+filter(key)).mkString("_")
    }
    MATCHID
  }else{
    MATCHID
  }
}
private[calc] object DataSetExpression{
  private val DataSet_REGEX: String = "#dataset\\s*\\[\\s*" +
    "value\\s*\\(\\s*(\\w+)\\.(\\w+)\\s*\\)(?:\\s*,\\s*" +
    "where\\s*\\(((?:\\s*\\w+\\s*,)*\\s*\\w+\\s*)\\s*\\)\\s*){0,1}(?:\\s*,\\s*" +
    "filter\\s*\\(((?:\\s*\\w+=[^,]+\\s*,)*\\s*\\w+=[^,]+\\s*)\\)\\s*){0,1}\\]"
  private val dataSetExpressionPattern: Pattern = Pattern.compile(DataSet_REGEX)

  private val Sum_DataSet_REGEX: String = "\\$\\.sum\\(\\s*\\$(\\d+)\\s*\\)"
  private val sumDataSetExpressionPattern: Pattern = Pattern.compile(Sum_DataSet_REGEX)

  private val Avg_DataSet_REGEX: String = "\\$\\.average\\(\\s*\\$(\\d+)\\s*\\)"
  private val avgDataSetExpressionPattern: Pattern = Pattern.compile(Avg_DataSet_REGEX)

  private val Other_DataSet_REGEX: String = "\\$\\.[^(sum)(average)][A-Za-z]{1}\\w*\\(\\s*\\$(\\d+)\\s*\\)"
  private val otherDataSetExpressionPattern: Pattern = Pattern.compile(Other_DataSet_REGEX)

  def getDataSetExpressions(formulaScript: String): Seq[DataSetExpression] = {
    if (null == formulaScript) return Seq()
    val matcher = dataSetExpressionPattern.matcher(formulaScript)
    val list = mutable.ListBuffer[DataSetExpression]()
    if (matcher != null) while (matcher.find) {
      //原始完整表达式
      val expression = matcher.group
      //数据集Id
      var dataSetId = matcher.group(1) // 数据集id
      dataSetId = if (dataSetId == null) null
      else dataSetId.replaceAll("\\s", "")
      var valueField = matcher.group(2) // 数据项id
      valueField = if (valueField == null) null
      else valueField.replaceAll("\\s", "")
      var whereExp = matcher.group(3) // wheres
      whereExp = if (whereExp == null) null
      else whereExp.replaceAll("\\s", "")
      var filterExp = matcher.group(4) // filters
      filterExp = if (filterExp == null) null
      else filterExp.replaceAll("\\s", "")
      val where: Seq[String] = if (whereExp != null && !"".equals(whereExp)) whereExp.split(",") else Seq[String]()
      val filter: Map[String, String] = if (filterExp != null && !"".equals(filterExp)) {
        var filerMap = Map[String, String]()
        filterExp.split(",").foreach(x => {
          filerMap += (x.split("=")(0) -> x.split("=")(1))
        })
        filerMap
      }else{
        Map[String, String]()
      }
      val dataSetExpression = new DataSetExpression(expression, dataSetId, valueField, where, filter)
      list += dataSetExpression
    }
    list
  }
  //sum算法
  def getSumDataSetIndex(formulaScript: String): Seq[(Int, String)] = {
    if (null == formulaScript) return Seq()
    val matcher = sumDataSetExpressionPattern.matcher(formulaScript)
    val list = mutable.ListBuffer[(Int, String)]()
    if (matcher != null) while (matcher.find) {
      list += ((matcher.group(1).toInt, matcher.group(0)))
    }
    list
  }
  //average算法
  def getAvgDataSetIndex(formulaScript: String): Seq[(Int, String)] = {
    if (null == formulaScript) return Seq()
    val matcher = avgDataSetExpressionPattern.matcher(formulaScript)
    val list = mutable.ListBuffer[(Int, String)]()
    if (matcher != null) while (matcher.find) {
      list += ((matcher.group(1).toInt, matcher.group(0)))
    }
    list
  }
  //其它算法
  def getOtherDataSetIndex(formulaScript: String): Seq[(Int, String)] = {
    if (null == formulaScript) return Seq()
    val matcher = otherDataSetExpressionPattern.matcher(formulaScript)
    val list = mutable.ListBuffer[(Int, String)]()
    if (matcher != null) while (matcher.find) {
      list += ((matcher.group(1).toInt, matcher.group(0)))
    }
    list
  }


}
//普通单值类型数据集
case class NormalDataSetExpression(pvalue: DataSetExpression) extends PreDataSetExpression{
  def value :DataSetExpression = pvalue
}
//集合求和单值类型数据集
case class SumDataSetExpression(pvalue: DataSetExpression) extends PreDataSetExpression{
  def value :DataSetExpression = pvalue
}
//求平均数单值类型数据集
case class AvgDataSetExpression(pvalue: DataSetExpression) extends PreDataSetExpression{
  def value :DataSetExpression = pvalue
}
//结果为集合类型的数据集
case class ListDataSetExpression(pvalue: DataSetExpression) extends PreDataSetExpression{
  def value :DataSetExpression = pvalue
}

trait PreDataSetExpression{
  def value :DataSetExpression
}
