package com.epri.dlsc.sbs.calc.datainstance

import org.apache.spark.sql.DataFrame

import scala.collection.mutable

private[calc] class DataSetInstance {
  private val dataSetInstanceCollectionMap = mutable.HashMap[String, DataFrame]()
  def get(dataSetId: String): DataFrame = {
    val dataSet = dataSetInstanceCollectionMap.getOrElse(dataSetId, null)
    require(dataSet != null, s"待计算的数据集【$dataSetId】加载失败")
    dataSet
  }
  def addOrUpdate(dataSetId: String, dataFrame: DataFrame): DataFrame = {
    dataFrame.cache()
    if(dataSetInstanceCollectionMap.contains(dataSetId)){
      dataSetInstanceCollectionMap(dataSetId).unpersist()
    }
    dataSetInstanceCollectionMap += (dataSetId -> dataFrame)
    dataFrame
  }
}


