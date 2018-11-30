package com.epri.dlsc.sbs.calc.test

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkReadFromOracleJoin {
  /**
    * args(0) 为partition数量
    */
  def main(args1: Array[String]): Unit = {

    val args = Array("50")

    if(args.length == 0) return

    val partitionsNumber = args(0)

    var num = 0
    try{
      num = partitionsNumber.toInt
    }catch {
      case _: NumberFormatException =>{
        println("第一个参数不能为非数字")
      }
    }
    if(num > 9999){
      println("partition数量设置过大")
      return
    }

    val spark: SparkSession = SparkSession
      .builder()
      .appName("Spark SQL Read Data From Oracle")
      .master("local[10]")
      .getOrCreate()

    val connectionProperties = new Properties()
    connectionProperties.put("user", "sgbiz")
    connectionProperties.put("password", "sgbiz")

    //table1
    val table1PartitionProp = new Array[String](num)
    for(i <- 0 to (num - 1)){
      table1PartitionProp(i) = "MOD(ORA_HASH(BUSIUNITID,10000)," +num+ ") = " + i
    }
    val ba_busiunit: DataFrame = spark.read.jdbc(
      "jdbc:oracle:thin:@10.6.0.85:1521:dlsc",
      "ba_busiunit",
      table1PartitionProp,
      connectionProperties)
    ba_busiunit.createOrReplaceTempView("BA_BUSIUNIT")
    //table2
    val table2PartitionProp = new Array[String](num)
    for(i <- 0 to (num - 1)){
      table2PartitionProp(i) = "MOD(ORA_HASH(SBS_UNIT_ID,10000)," +num+ ") = " + i
    }
    val se_result_n_m: DataFrame = spark.read.jdbc(
      "jdbc:oracle:thin:@10.6.0.85:1521:dlsc",
      "se_result_n_m",
      table2PartitionProp,
      connectionProperties)
    se_result_n_m.createOrReplaceTempView("SE_RESULT_N_M")
    //table3
    val table3PartitionProp = new Array[String](num)
    for(i <- 0 to (num - 1)){
      table3PartitionProp(i) = "MOD(ORA_HASH(CONTRACTID,10000)," +num+ ") = " + i
    }
    val co_contractbaseinfo: DataFrame = spark.read.jdbc(
      "jdbc:oracle:thin:@10.6.0.85:1521:dlsc",
      "co_contractbaseinfo",
      table3PartitionProp,
      connectionProperties)
    co_contractbaseinfo.createOrReplaceTempView("CO_CONTRACTBASEINFO")

        val startTime = System.currentTimeMillis()

    //join table1,table2 and table3
    val result: DataFrame = spark.sql(
        "SELECT SBS_UNIT_ID,BUSIUNITNAME,M.CONTRACT_ID,CONTRACTNAME,ENERGY_T "+
        "FROM SE_RESULT_N_M M,BA_BUSIUNIT B,CO_CONTRACTBASEINFO C "+
        "WHERE M.SBS_UNIT_ID = B.BUSIUNITID "+
        "AND M.CONTRACT_ID = C.CONTRACTID"
      )

    result.show(10)

    val endTime = System.currentTimeMillis()
    println("共耗时:" + (endTime - startTime)/1000)

    spark.sparkContext.stop
  }

}
