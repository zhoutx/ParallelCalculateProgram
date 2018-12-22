package com.epri.dlsc.sbs.calc.service


import java.util
import java.util.UUID
import java.util.regex.Pattern

import com.epri.dlsc.sbs.calc.DataSet._
import com.epri.dlsc.sbs.calc.config.Configuration
import com.epri.dlsc.sbs.calc.datainstance.{DataSetInstance, OracleDataSet}
import com.epri.dlsc.sbs.calc.formula.Formula
import com.sgcc.dlsc.sbs.functions.`type`.DataSetRow
import javax.script.{Compilable, ScriptContext, ScriptEngineManager, SimpleBindings}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

import scala.collection.JavaConversions.seqAsJavaList
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

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
  /**
    * 公式分析
    * @param formula 公式
    * @return Seq[(主公式信息,(计算项,seq[数据集],预处理的公式内容))]
    * 预处理的公式内容:
    * 1+2+3+dateset[...]+$.sum(dateset[...])+$.average(dateset[...])+$.toArrayObject(dateset[...]) => (function(){1+2+3+$1+$2+$3+$.toArrayObject($4)})()
    */
  private def formulaAnalyze(formula: Formula): (Formula, Seq[(String, Seq[PreDataSetExpression], String)]) = {
    val calcFields = formula.formulaItems
    val rightCon = calcFields.map(formulaItem => {
      val calcField = formulaItem.field
      var formulaContent = formulaItem.formulaContent
      //所有使用到的数据集
      val dataSetList = DataSetExpression.getDataSetExpressions(formulaContent)
      //1+2+3+dateset[...]+$.sum(dateset[...])+$.average(dateset[...])+$.toArrayObject(dateset[...])
      // => 1+2+3+$1+$.sum($2)+$.average($3)+$.toArrayObject($4)
      for(i <- dataSetList.indices){
        formulaContent = Pattern.compile(dataSetList(i).expression, Pattern.LITERAL).matcher(formulaContent).replaceFirst("\\$"+(i+1))
      }
      val sumDataSetList = DataSetExpression.getSumDataSetIndex(formulaContent)
      // => 1+2+3+$1+$2+$.average($3)+$.toArrayObject($4)
      sumDataSetList.foreach(x => {
        formulaContent = formulaContent.replace(x._2, "$"+x._1)
      })
      val avgDataSetList = DataSetExpression.getAvgDataSetIndex(formulaContent)
      // => 1+2+3+$1+$2+$3+$.toArrayObject($4)
      avgDataSetList.foreach(x => {
        formulaContent = formulaContent.replace(x._2, "$"+x._1)
      })
      //=> (function(){1+2+3+$1+$2+$3+$.toArrayObject($4)})()
      formulaContent = s"(function(){$formulaContent})()"
      val otherDataSetList = DataSetExpression.getOtherDataSetIndex(formulaContent)
      val sumDataSetIdx = sumDataSetList.map(_._1).toSet
      val avgDataSetIdx = avgDataSetList.map(_._1).toSet
      val listDataSetIdx = otherDataSetList.map(_._1).toSet
      val preDataSetList = for(i <- dataSetList.indices) yield {
        val dataSet = dataSetList(i)
        if(sumDataSetIdx.contains(i+1)) SumDataSetExpression(dataSet)
        else if(avgDataSetIdx.contains(i+1)) AvgDataSetExpression(dataSet)
        else if(listDataSetIdx.contains(i+1)) ListDataSetExpression(dataSet)
        else NormalDataSetExpression(dataSet)
      }
      (calcField, preDataSetList, formulaContent)
    })
    (formula, rightCon)
  }

  /**
    * @param preparedFormula Seq(公式基本信息, Seq[(计算项, Seq[数据集], 公式)])
    * @return
    */
  private def execute(preparedFormula: Seq[(Formula, Seq[(String, Seq[PreDataSetExpression], String)])]): Boolean = {
    preparedFormula.foreach(formula => {
      val formulaInfo = formula._1
      //单个主公式范围内的缓存数据
      val scopeCache = ScopeDataCache.getInstance
      val filters = formulaInfo.filters

      val calcLeftDataFull = dataSetInstance.get(formulaInfo.calcDataSetId)

      val calcLeftData = if (filters.isEmpty) {
        calcLeftDataFull
      } else {
        calcLeftDataFull.filter(row => {
          var isTrue = true
          filters.foreach(filter => {
            val columnName = filter._1
            val value = filter._2
            val rowFilterValue = row.get(row.fieldIndex(columnName))
            if(rowFilterValue.isInstanceOf[Double]){
              isTrue = isTrue && java.lang.Double.valueOf(value) == rowFilterValue
            }else{
              isTrue = isTrue && value.equals(rowFilterValue.toString)
            }
          })
          isTrue
        })
      }

      formula._2.foreach(item => {
        //计算项
        val calcField = item._1
        //公式中的数据集
        val matchedDataSets: Seq[PreDataSetExpression] = item._2
        //预处理的公式内容
        val preparedFormulaContent = item._3

        var newLeftData: RDD[(String, Double)] = null
        if (matchedDataSets.size < 1) {
          //(rowid, value)
          newLeftData = executeWithoutDataSet(calcLeftData, calcField, preparedFormulaContent)
        } else {
          //(rowid, value)
          newLeftData = executeWithDataSet(formulaInfo, calcLeftData, matchedDataSets, preparedFormulaContent, scopeCache)
        }
        //计算后数据更新
        newLeftData.cache()
        scopeCache.addSelf(calcField, newLeftData)
        val valueDF = sparkSession.createDataFrame(newLeftData.map(row => {
          Row(row._1, row._2)
        }), StructType(Array(StructField("ROWID", StringType), StructField("value", DoubleType))))

        val newJoinRdd = calcLeftDataFull.join(valueDF, Array("ROWID"), "left_outer")
        val newRows = newJoinRdd.rdd.map(row => {
            val values = for(i <- 0 until row.size) yield {
            if(row.fieldIndex(calcField) == i){
              val modifyValue = row.get(row.fieldIndex("value"))
              if(modifyValue == null){
                row.get(i)
              }else{
                modifyValue
              }
            }else{
              row.get(i)
            }
          }
          Row(values: _*)
        })
        val newLeftDataDF = sparkSession.createDataFrame(newRows, newJoinRdd.schema).drop("value")
        dataSetInstance.addOrUpdate(formulaInfo.calcDataSetId, newLeftDataDF)
        requirePersistDF.add(formulaInfo.calcDataSetId, newLeftDataDF)
      })
    })
    //数据存储
    requirePersistDF.list().foreach(_.show())


    true
  }
  //不包含数据集的公式计算
  private def executeWithoutDataSet(
       calcLeftData: DataFrame,
       modifyColumn: String,
       formulaScript: String): RDD[(String, Double)] = {
    val exeFormulaScript = formulaScript
    calcLeftData.rdd.mapPartitions(partition => {
      val scriptEngine = new ScriptEngineManager().getEngineByName("javascript")
      val result = scriptEngine.eval(exeFormulaScript)
      if(result != null && result.toString.length > 0){
        partition.map(row => (row.getAs[String]("ROWID"), result.toString.toDouble))
      }else{
        partition.map(row => (row.getAs[String]("ROWID"), row.getAs[Double](modifyColumn)))
      }
    })
  }
  //包含数据集的公式计算
  private def executeWithDataSet(
      formula: Formula,
      calcLeftData: DataFrame,
      rightDataSets: Seq[PreDataSetExpression],
      formulaScript: String,
      scopeDataCache: ScopeDataCache): RDD[(String, Double)] = {
    val exeFormulaScript = formulaScript
    //Seq[RDD(String, Double/DataSetRow)]
    val rightDataSetValueList: Seq[AnyRef] = rightDataSets.map {
      case rightDataSet@NormalDataSetExpression(_) => normalDataSetHandle(formula, scopeDataCache, calcLeftData, rightDataSet)
      case rightDataSet@SumDataSetExpression(_) => sumDataSetHandle(scopeDataCache, calcLeftData, rightDataSet)
      case rightDataSet@AvgDataSetExpression(_) => avgDataSetHandle(scopeDataCache, calcLeftData, rightDataSet)
      case rightDataSet@ListDataSetExpression(_) => listDataSetHandle(calcLeftData, rightDataSet)
    }

    var joinLeftRDDs = rightDataSetValueList.head.asInstanceOf[RDD[(String, Any)]]
    for(idx <- 1 until rightDataSetValueList.size){
      val joinRightRDDs = rightDataSetValueList(idx).asInstanceOf[RDD[(String, Any)]]
      joinLeftRDDs = joinLeftRDDs.join(joinRightRDDs).asInstanceOf[RDD[(String, Any)]]
    }
    val calcParams = if(rightDataSetValueList.size > 1){
      joinLeftRDDs.map(row => {
        val list = mutable.ListBuffer[Any]()
        var isTuple = row._2.asInstanceOf[(Any, Any)]
        list += isTuple._2
        while(isTuple._1.isInstanceOf[(Any, Any)]){
          isTuple = isTuple._1.asInstanceOf[(Any, Any)]
          list += isTuple._2
        }
        list +=  isTuple._1
        (row._1, list)
      })
    }else{
      joinLeftRDDs.map(row => (row._1, List[Any](row._2)))
    }

    val itemResult = calcParams.mapPartitions(partition => {
      val scriptEngine = new ScriptEngineManager().getEngineByName("javascript")
      val formulaAlgorithm = new util.HashMap[String, AnyRef]()
      formulaAlgorithm.put("$", new com.sgcc.dlsc.sbs.functions.inner.STD())
      scriptEngine.setBindings(new SimpleBindings(formulaAlgorithm), ScriptContext.GLOBAL_SCOPE)

      val compiledScript = scriptEngine.asInstanceOf[Compilable].compile(exeFormulaScript)
      partition.map(row => {
        val params = new util.HashMap[String, Object]()
        var i = 1
        for(param <- row._2){
          params.put("$"+i, if(param != null) param.asInstanceOf[Object] else null)
          i +=1
        }
        val value = compiledScript.eval(new SimpleBindings(params))
        (row._1,  if(value == null) null else java.lang.Double.valueOf(value.toString))
      })
    })
    itemResult.asInstanceOf[RDD[(String, Double)]]
  }

  //处理一对一匹配数据集
  private def normalDataSetHandle(
       formula: Formula,
       scopeDataCache: ScopeDataCache,
       calcLeftData: DataFrame,
       rightDataSet: PreDataSetExpression): RDD[(String, Double)] = {
    val rightCalcField = rightDataSet.value.valueField
    val rightFilter = rightDataSet.value.filter
    val rightDataSetId = rightDataSet.value.dataSetId
    val rightWhere = rightDataSet.value.where
    //如果公式右边某个数据集就是公式左边数据集，直接返回或者从缓存中取值
    if(formula.calcDataSetId.equals(rightDataSetId)){
      val cacheData = scopeDataCache.getSelf(rightCalcField)
      if(cacheData == null)
        return calcLeftData.rdd.map(row => (row.getAs[String]("ROWID"), row.getAs[Double](rightCalcField)))
      else
        return cacheData
    }
    //否则进行下面的计算
    //取缓存数据
    val cacheData = scopeDataCache.getNormal(rightDataSet.value.MATCH_IDENTIFIER, rightCalcField)
    if (cacheData == null) {
      val calcRightData = if (rightFilter.isEmpty) {
        dataSetInstance.get(rightDataSetId)
      } else {
        dataSetInstance.get(rightDataSetId).filter(row => {
          var isTrue = true
          rightFilter.foreach(filter => {
            val columnName = filter._1
            val value = filter._2
            val rowFilterValue = row.get(row.fieldIndex(columnName))
            if(rowFilterValue.isInstanceOf[Double]){
              isTrue = isTrue && java.lang.Double.valueOf(value) == rowFilterValue
            }else{
              isTrue = isTrue && value.equals(rowFilterValue.toString)
            }
          })
          isTrue
        })
      }
      val leftDataRequireColData = calcLeftData.select(calcLeftData.col("ROWID") :: rightWhere.map(calcLeftData.col).toList: _*)
      val rightDataRequireColData = calcRightData.select(calcRightData.columns.filter(!"ROWID".equals(_)).map(calcRightData.col): _*)
      val joinData = leftDataRequireColData.join(rightDataRequireColData, rightWhere, "left_outer").cache()
      //缓存数据
      scopeDataCache.addNormal(rightDataSet.value.MATCH_IDENTIFIER, joinData)
      //返回
      joinData.rdd.map(row => (row.getAs[String]("ROWID"), row.getAs[Double](rightCalcField)))
    } else {
      cacheData
    }
  }
  //处理一对多聚合求和
  private def sumDataSetHandle(
      scopeDataCache: ScopeDataCache,
      calcLeftData: DataFrame,
      rightDataSet: PreDataSetExpression): RDD[(String, Double)] ={
    val rightCalcField = rightDataSet.value.valueField
    val rightFilter = rightDataSet.value.filter
    val rightDataSetId = rightDataSet.value.dataSetId
    val rightWhere = rightDataSet.value.where
    //取缓存数据
    val cacheData = scopeDataCache.getSUM(rightDataSet.value.MATCH_IDENTIFIER)
    if(cacheData == null){
      val calcRightData = if (rightFilter.isEmpty) {
        dataSetInstance.get(rightDataSetId)
      } else {
        dataSetInstance.get(rightDataSetId).filter(row => {
          var isTrue = true
          rightFilter.foreach(filter => {
            val columnName = filter._1
            val value = filter._2
            val rowFilterValue = row.get(row.fieldIndex(columnName))
            if(rowFilterValue.isInstanceOf[Double]){
              isTrue = isTrue && java.lang.Double.valueOf(value) == rowFilterValue
            }else{
              isTrue = isTrue && value.equals(rowFilterValue.toString)
            }
          })
          isTrue
        })
      }
      val rightSumData = calcRightData.groupBy(rightWhere.map(calcRightData(_)): _*).agg(sum(rightCalcField) as "value")
      val returnRdd = calcLeftData.join(rightSumData, rightWhere, "left_outer").select(calcLeftData("ROWID"), rightSumData("value"))
                        .rdd.map(row => (row.getAs[String]("ROWID"), row.getAs[Double]("value"))).cache()
      //缓存数据
      scopeDataCache.addSUM(rightDataSet.value.MATCH_IDENTIFIER, returnRdd)
      //返回
      returnRdd
    }else{
      cacheData
    }
  }
  //处理一对多聚合平均值
  private def avgDataSetHandle(
      scopeDataCache: ScopeDataCache,
      calcLeftData: DataFrame,
      rightDataSet: PreDataSetExpression): RDD[(String, Double)] = {
    val rightCalcField = rightDataSet.value.valueField
    val rightFilter = rightDataSet.value.filter
    val rightDataSetId = rightDataSet.value.dataSetId
    val rightWhere = rightDataSet.value.where
    //取缓存数据
    val cacheData = scopeDataCache.getAVG(rightDataSet.value.MATCH_IDENTIFIER)
    if(cacheData == null){
      val calcRightData = if (rightFilter.isEmpty) {
        dataSetInstance.get(rightDataSetId)
      } else {
        dataSetInstance.get(rightDataSetId).filter(row => {
          var isTrue = true
          rightFilter.foreach(filter => {
            val columnName = filter._1
            val value = filter._2
            val rowFilterValue = row.get(row.fieldIndex(columnName))
            if(rowFilterValue.isInstanceOf[Double]){
              isTrue = isTrue && java.lang.Double.valueOf(value) == rowFilterValue
            }else{
              isTrue = isTrue && value.equals(rowFilterValue.toString)
            }
          })
          isTrue
        })
      }
      val rightAvgData = calcRightData.groupBy(rightWhere.map(calcRightData(_)): _*).agg(avg(rightCalcField) as "value")
      val returnRdd = calcLeftData.join(rightAvgData, rightWhere, "left_outer").select(calcLeftData("ROWID"), rightAvgData("value"))
                    .rdd.map(row => (row.getAs[String]("ROWID"), row.getAs[Double]("value"))).cache()
      //缓存数据
      scopeDataCache.addAVG(rightDataSet.value.MATCH_IDENTIFIER, returnRdd)
      //返回
      returnRdd
    }else{
      cacheData
    }
  }
  //处理一对多聚合List
  private def listDataSetHandle(
       calcLeftData: DataFrame,
       rightDataSet: PreDataSetExpression): RDD[(String, java.util.List[DataSetRow])] = {
    val rightCalcField = rightDataSet.value.valueField
    val rightFilter = rightDataSet.value.filter
    val rightDataSetId = rightDataSet.value.dataSetId
    val rightWhere = rightDataSet.value.where
    //没有缓存数据
    val calcRightData = if (rightFilter.isEmpty) {
      dataSetInstance.get(rightDataSetId)
    } else {
      dataSetInstance.get(rightDataSetId).filter(row => {
        var isTrue = true
        rightFilter.foreach(filter => {
          val columnName = filter._1
          val value = filter._2
          val rowFilterValue = row.get(row.fieldIndex(columnName))
          if(rowFilterValue.isInstanceOf[Double]){
            isTrue = isTrue && java.lang.Double.valueOf(value) == rowFilterValue
          }else{
            isTrue = isTrue && value.equals(rowFilterValue.toString)
          }
        })
        isTrue
      })
    }
    val leftData = calcLeftData.rdd.map(row => {
      var key = row.get(row.fieldIndex(rightWhere.head)).toString
      key = if(rightWhere.size > 1) {
              for(i <- 1 until rightWhere.size){
                key += row.get(row.fieldIndex(rightWhere(i))).toString
              }
              key
            }else{
              key
            }
      (key, row.getAs[String]("ROWID"))
    })
    val rightData = calcRightData.rdd.groupBy(row => {
      var key = row.get(row.fieldIndex(rightWhere.head)).toString
      if(rightWhere.size > 1) {
        for(i <- 1 until rightWhere.size){
          key += row.get(row.fieldIndex(rightWhere(i))).toString
        }
        key
      }else{
        key
      }
    })
    val fieldNames = calcRightData.schema.fieldNames
    leftData.leftOuterJoin(rightData).map(row => {
      val rowId = row._2._1
      val itr = row._2._2.orNull
      if(itr != null){
        val list = itr.toList.map(row => {
          val dataSetRow = row.getValuesMap(fieldNames).map(entry=>entry._1 -> entry._2)
          val aa = new util.HashMap[String, String]()
          dataSetRow.foreach(kv => aa.put(kv._1, if(kv._2 != null) kv._2.toString else null))
          new DataSetRow(rightCalcField, aa)
        })
        (rowId, list)
      }else{
        (rowId, null)
      }
    })
  }

  /**
    * 加载数据集数据实体
    * @param preparedFormula 预处理的计算公式相关内容
    */
  private def dataSetInstanceLoad(preparedFormula: Seq[(Formula, Seq[(String, Seq[PreDataSetExpression], String)])]): Unit={
    val usedDataSetIds = mutable.Set[String]()
    preparedFormula.foreach(x => {
      usedDataSetIds += x._1.calcDataSetId
      x._2.foreach(y => {
        usedDataSetIds ++= y._2.map(z => z.value.dataSetId)
      })
    })
    val dataSetConfigCollectionMap = mutable.HashMap[String, DataSet]()
    val dataSets = Configuration.queryDataSets(config.marketId, config.sourceTarget, config.scriptParams, usedDataSetIds.toList)
    dataSets.foreach(dataSet => {
        dataSetConfig.add(dataSet)
        dataSetConfigCollectionMap += (dataSet.id -> dataSet)
    })
    //将数据集基本信息广播
    dataSetBroadcastVar = sparkSession.sparkContext.broadcast[mutable.HashMap[String, DataSet]](dataSetConfigCollectionMap)
    dataSets.foreach(dataSet => {
      val instance = OracleDataSet.getInstance(sparkSession, dataSetBroadcastVar, dataSet)
      dataSetInstance.addOrUpdate(dataSet.id, instance)
    })


  }

  /**
    * 计算任务提交
    * @return
    */
  def submit(): Unit = {
    val formulas = Configuration.queryFormuals(config.marketId, config.formulaIds)
    val preparedFormula = formulas.map(formulaAnalyze)
    dataSetInstanceLoad(preparedFormula)
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
  private val sparkSession: SparkSession = SparkSession.builder().config("spark.debug.maxToStringFields", 100).appName("Settle System").master("local[10]").getOrCreate()
  private val dataSetInstance = new DataSetInstance()
  private val requirePersistDF = new RequirePersistDF()
  private val dataSetConfig = new DataSetConfig()

  private var dataSetBroadcastVar: Broadcast[mutable.HashMap[String, DataSet]] = _
  private val formulaConfigCollection= mutable.HashMap[String, Formula]()

  private class DataSetConfig{
    private val dataSetConfigCollection = mutable.HashMap[String, DataSet]()
    def add(dataSet: DataSet): Unit = dataSetConfigCollection += (dataSet.id -> dataSet)
    def apply(dataSetId: String): DataSet ={
      val data = dataSetConfigCollection.getOrElse(dataSetId, null)
      require(data != null, s"待计算的数据集【$dataSetId】加载失败")
      data
    }

  }

  private class RequirePersistDF{
    private val persistDF = mutable.LinkedHashMap[String, DataFrame]()
    def add(key: String, df: DataFrame): this.type = {
      if(persistDF.contains(key)){
        persistDF -= key
      }
      persistDF += (key -> df)
      this
    }
    def list(): Seq[DataFrame] = persistDF.values.toList
  }

  class ScopeDataCache{
    private val rddDataCache = mutable.HashMap[String, RDD[(String, Double)]]()
    private val dataFrameCache = mutable.HashMap[String, DataFrame]()
    def addNormal(key: String, value: DataFrame): Unit = dataFrameCache += (ScopeDataCache.prefix_NORMAL + key -> value)
    def getNormal(key: String, valueField: String): RDD[(String, Double)] = {
      val value = dataFrameCache.getOrElse(ScopeDataCache.prefix_NORMAL + key, null)
      if(value == null){
        null
      }else{
        value.rdd.map(row => (row.getAs[String]("ROWID"), row.getAs[Double](valueField)))
      }
    }
    def addSUM(key: String, value: RDD[(String, Double)]): Unit = rddDataCache += (ScopeDataCache.prefix_SUM + key -> value)
    def getSUM(key: String): RDD[(String, Double)] = rddDataCache.getOrElse(ScopeDataCache.prefix_SUM + key, null)

    def addAVG(key: String, value: RDD[(String, Double)]): Unit = rddDataCache += (ScopeDataCache.prefix_AVG+ key -> value)
    def getAVG(key: String): RDD[(String, Double)] = rddDataCache.getOrElse(ScopeDataCache.prefix_AVG + key, null)

    //暂时不实现
    def getList(key: String): (String, Seq[DataSetRow]) = null

    def addSelf(key: String, value: RDD[(String, Double)]): Unit = rddDataCache += (ScopeDataCache.prefix_SELF + key -> value)
    def getSelf(key: String): RDD[(String, Double)] = rddDataCache.getOrElse(ScopeDataCache.prefix_SELF + key, null)
  }
  object ScopeDataCache{
    private val prefix_NORMAL = "NORMAL"
    private val prefix_SUM = "SUM"
    private val prefix_AVG = "AVG"
    private val prefix_LIST = "LIST"
    private val prefix_SELF = "SELF"
    def getInstance: ScopeDataCache = new ScopeDataCache()
  }
}

class ScriptParams private(
    cParamName: String,
    cParamValue: String){
  private var params = immutable.Map[String, String](cParamName -> cParamValue)
  def add(paramName: String, paramValue: String): this.type = {
    params += (paramName -> paramValue)
    this
  }
  private[calc] def toMap: Map[String, String] = params
}
object ScriptParams{
  def add(paramName: String, paramValue: String): ScriptParams = new ScriptParams(paramName, paramValue)
}

class SourceTarget(val source: String, val target: String)
object SourceTarget{
  def apply(source: String, target: String): SourceTarget = new SourceTarget(source, target)
}