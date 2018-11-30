package com.epri.dlsc.sbs.calc.DataSet

import java.util.regex.Pattern

import scala.collection.mutable.ListBuffer


object ScriptParamExpression {
  val STATIC = 0
  val VARIABLE = 1

  object ScriptParamExpressionParser {
    private val REGEX1 = "\\$\\{\\s*(\\S+)\\s*\\}"
    private val REGEX2 = "#\\{\\s*(\\S+)\\s*\\}"
    private val PATTERN1 = Pattern.compile(REGEX1)
    private val PATTERN2 = Pattern.compile(REGEX2)

    def getExpressions(exps: String): Seq[ScriptParamExpression] = {
      var list: ListBuffer[ScriptParamExpression] = ListBuffer[ScriptParamExpression]()
      if (null == exps) return list
      val matcher1 = PATTERN1.matcher(exps)
      if (matcher1 != null){
        while (matcher1.find){
          val expression = matcher1.group
          // 完整表达式
          val paramName = matcher1.group(1)
          // sql条件属性名
          val sqlCondExps = new ScriptParamExpression(paramName, expression, ScriptParamExpression.VARIABLE)
          list += sqlCondExps
        }
      }
      val matcher2 = PATTERN2.matcher(exps)
      if (matcher2 != null){
        while (matcher2.find) {
          if (list == null) list = ListBuffer[ScriptParamExpression]()
          val expression = matcher2.group
          val paramName = matcher2.group(1)
          val sqlCondExps = ScriptParamExpression(paramName, expression, ScriptParamExpression.STATIC)
          list += sqlCondExps
        }
      }
      list
    }
  }
  def apply(
      getParamName: String,
      getParamExpression: String,
      getParamType: Int): ScriptParamExpression = new ScriptParamExpression(getParamName, getParamExpression, getParamType)
}

class ScriptParamExpression(
    val paramName: String,
    val paramExpression: String,
    val paramType: Int) {
}

