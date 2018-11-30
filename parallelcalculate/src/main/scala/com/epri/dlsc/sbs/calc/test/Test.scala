package com.epri.dlsc.sbs.calc.test

import java.io.{BufferedReader, Reader, StringReader}
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties

import javax.script.{Compilable, CompiledScript, ScriptEngineManager, SimpleBindings}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer


object Test {

  def main(args: Array[String]): Unit = {

    val connection = DriverManager.getConnection("jdbc:oracle:thin:@10.6.0.85:1521:dlsc", "zjsgbiz", "zjsgbiz")
    val sql =
      """
        |select a.dataset_name,b.sql
        |from se_upg_dataset_define a,
        |     se_upg_dataset_source b
        |where a.id = b.dataset_id
      """.stripMargin
    val statement: PreparedStatement = connection.prepareStatement(sql)
    val resultSet: ResultSet = statement.executeQuery()
    var sqlList = scala.collection.mutable.ListBuffer[(String, String)]()
    while(resultSet.next()){
      val script = resultSet.getClob("SQL")
      val stream: Reader = script.getCharacterStream
      val sql = new Array[Char](script.length().toInt)
      stream.read(sql)
      stream.close()
      val dataSetName = resultSet.getString("DATASET_NAME")
      sqlList += ((dataSetName, new String(sql)))
    }
    val dataSets = sqlList.map(dataSet => {
      val dataSetName = dataSet._1
      val sql = "("+ dataSet._2.replace("${taskid}", "11111")
        .replace("${tradeseqid}", "'20180115000002'")
        .replace("${datetime}", "'2018-01-15'")
        .replace("${starttime}", "'2018-01-15 00:00:00'")
        .replace("${endtime}", "'2018-01-15 23:45:00'")+")"
      (dataSetName, sql)
    })

    val spark: SparkSession = SparkSession
      .builder()
      .appName("Spark SQL Read Data From Oracle")
      .master("local[10]")
      .getOrCreate()

    import spark.implicits._

    val connectionProperties = new Properties()
    connectionProperties.put("user", "zjsgbiz")
    connectionProperties.put("password", "zjsgbiz")

//    val dataFrameList = dataSets.map(dataSet => {
//      (dataSet._1, spark.read.jdbc("jdbc:oracle:thin:@10.6.0.85:1521:dlsc", dataSet._2, connectionProperties).repartition(10))
//    })
//
    val starttime = System.currentTimeMillis()
//    val result = dataFrameList.map(dataSet =>{
//      (dataSet._2.count(), dataSet._1)
//    })
    val rdd1 = spark.read.jdbc("jdbc:oracle:thin:@10.6.0.85:1521:dlsc",
     """
       |(SELECT O.TRADESEQ_ID,
       |       O.TAG_PHY,
       |       O.CAPTION,
       |       O.DATA_TIME,
       |       O.RT_LOAD,
       |       O.TOTAL_RT_LOAD,
       |       O.Tier2_Synchres_Sales,
       |       O.Tier2_Synchres_Purchase,
       |       O.MARKET_ID,
       |       O.CASE_ID,
       |       NULL ASSIGNED_TIER2_OBLI,
       |       NULL TOTAL_TIER2_OBLI,
       |       NULL ADJUSTED_TIER2_OBLI,
       |       NULL TOTAL_SRMCP_CREDIT,
       |       NULL SRMCP_CHARGE,
       |       NULL SELF_SCHEDULED_TIER2,
       |       NULL TIER2_PURCHASE,
       |       NULL TOTAL_TIER2_PURCHASE,
       |       NULL TOTAL_TIER2_LOC_CREDIT,
       |       NULL TIER2_LOC_CHARGE,
       |       NULL TIER2_CHARGE,SYS_GUID() GUID
       |  FROM SE_RT_RES_CON_OBLI O
       |   WHERE DATA_TIME BETWEEN to_date('2018-01-15 00:00:00', 'yyyy-mm-dd hh24:mi:ss') and
       |       to_date('2018-01-15 23:45:00', 'yyyy-mm-dd hh24:mi:ss'))
     """.stripMargin, connectionProperties).rdd.map(row => {
            (row.getAs[String]("TAG_PHY"), row.getAs[Number]("RT_LOAD"))
          }).cache()
    val rdd2 = rdd1.map(row => (row._1 + "1", row._2 + "1"))
    val rdd3 = rdd1.map(row => (row._1 + "2", row._2 + "2"))
    val rdd4 = rdd1.map(row => (row._2 + "99", row._1 + "99"))
    val rdd5 = rdd1.map(row => (row._1 + "1", row._2 + "1"))
    val rdd6 = rdd1.map(row => (row._1 + "2", row._2 + "2"))
    val rdd7 = rdd1.map(row => (row._2 + "99", row._1 + "99"))
    val c1 = rdd2.count()
    val c2 = rdd3.count()
    val c3 = rdd4.count()
    val c4 = rdd5.count()
    val c5 = rdd6.count()
    val c6 = rdd7.count()

    println(c1)
    println(c2)
    println(c3)
    println(c4)
    println(c5)
    println(c6)

    rdd2.take(10).foreach(println(_))
    rdd3.take(10).foreach(println(_))
    rdd4.take(10).foreach(println(_))
    rdd5.take(10).foreach(println(_))
    rdd6.take(10).foreach(println(_))
    rdd7.take(10).foreach(println(_))



    println("耗时："  + (System.currentTimeMillis() - starttime) + "ms")
    spark.close()


  }




}

