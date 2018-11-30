package com.epri.dlsc.sbs.calc.datainstance

import java.util.Properties

import com.epri.dlsc.sbs.calc.DataSet.DataSet
import com.epri.dlsc.sbs.calc.config.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}


class OracleDataSetInstance(
     val dataSet: DataSet,
     val dataRDD: DataFrame)
object OracleDataSourceRDD{
  def apply(
     dataSet: DataSet,
     dataRDD: DataFrame): OracleDataSetInstance =
    new OracleDataSetInstance(dataSet, dataRDD)
}

class OracleDataSourceLoader(
     sparkSession: SparkSession,
     dataSet: DataSet) {

  def load: OracleDataSetInstance = {
    val sql = dataSet.script
    if (sql == null) throw new RuntimeException(s"数据集[${dataSet.name}]没定义数据源脚本语句")
    val dbConn = new Properties()
    dbConn.setProperty("user", Configuration.user)
    dbConn.setProperty("password", Configuration.pwd)
    val originDataFrame = sparkSession.read.jdbc(
      Configuration.url,
      s"($sql)",
      dbConn)

  }
}
object OracleDataSourceLoader{
  def apply(
    sparkSession: SparkSession,
    dataSet: DataSet): OracleDataSourceLoader =
    new OracleDataSourceLoader(sparkSession, dataSet)


}


