package com.epri.dlsc.sbs.calc.datasource

import java.util.Properties

import com.epri.dlsc.sbs.calc.DataSet.DataSet
import com.epri.dlsc.sbs.calc.config.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}


class OracleDataSource(
     val dataSetId: String,
     val dataRDD: DataFrame,
     val constraintFieldNames: Seq[String])
object OracleDataSourceRDD{
  def apply(
     dataSetId: String,
     dataRDD: DataFrame,
     constraintFieldNames: Seq[String]): OracleDataSource =
    new OracleDataSource(dataSetId, dataRDD, constraintFieldNames)
}

class OracleDataSourceLoader(
     sparkSession: SparkSession,
     dataSet: DataSet) {

  def load: OracleDataSource = {
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


