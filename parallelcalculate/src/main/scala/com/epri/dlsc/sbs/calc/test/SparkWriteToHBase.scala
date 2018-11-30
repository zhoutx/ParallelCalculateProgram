package com.epri.dlsc.sbs.calc.test

import java.sql.{Connection, DriverManager, ResultSet, Statement}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.apache.spark.sql.{DataFrame, SparkSession}


object SparkWriteToHBase {
  def main(args1: Array[String]): Unit = {


//    val config = HBaseConfiguration.create();
//    config.set("hbase.zookeeper.quorum", "10.6.0.61");
//
//    val connection = ConnectionFactory.createConnection(config);
//    val admin = connection.getAdmin();
//
//    // list the tables
//    val listtables=admin.listTables()
//    listtables.foreach(println)


//    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
//    val conn : Connection = DriverManager.getConnection("jdbc:phoenix:node1,node2,node3:2181")
//    val statement: Statement = conn.createStatement
//    val time: Long = System.currentTimeMillis
//    val sql: String = "select * from \"test\""
//    val rs: ResultSet = statement.executeQuery(sql)
//    while (rs.next) {
//      println(rs.getObject("name"))
//    }
//    rs.close()
//    statement.close()
//    conn.close()
//    val timeUsed: Long = System.currentTimeMillis - time
//    System.out.println("所花费的时间" + timeUsed)



    val args = Array("50")

    if(args.length == 0) return

    val spark: SparkSession = SparkSession
      .builder()
      .appName("Spark SQL Read Data From Hbase")
      .master("local[*]")
      .getOrCreate()
    val testDF : DataFrame = spark.read.format("org.apache.phoenix.spark")
                                       .option("table", "\"test\"")
                                       .option("zkUrl", "node1,node2,node3:2181")
                                       .load()



    testDF.show()
//    val partitionsNumber = args(0)
//
//    var num = 0
//    try{
//      num = partitionsNumber.toInt
//    }catch {
//      case _: NumberFormatException =>{
//        println("第一个参数不能为非数字")
//      }
//    }
//    if(num > 9999){
//      println("partition数量设置过大")
//      return
//    }
//
//    val partitionProp = new Array[String](num)
//    for(i <- 0 to (num - 1)){
//      partitionProp(i) = "ROWNUM <= 10000 AND MOD(ORA_HASH(GUID,10000)," +num+ ") = " + i
//    }
//
//    val spark: SparkSession = SparkSession
//      .builder()
//      .appName("Spark SQL Read Data From Oracle")
//      .master("local[10]")
//      .getOrCreate()
//
//    val connectionProperties = new Properties()
//    connectionProperties.put("user", "sgbiz")
//    connectionProperties.put("password", "sgbiz")
//
//    val jdbcDF: DataFrame = spark.read.jdbc(
//      "jdbc:oracle:thin:@10.6.0.85:1521:dlsc",
//      "TEST_100MILLION_v",
//      partitionProp,
//      connectionProperties)
//
//    jdbcDF.createOrReplaceTempView("TEST_100MILLION")
//
//    val sqlDF: DataFrame = spark.sql("SELECT GUID,BUSIUNITNAME,BUSIUNITID,SBS_TYPE_NAME,ENERGY_T,PRICE_T,FEE_T FROM TEST_100MILLION")
//
//    sqlDF.show(10)
//
//    val startTime = System.currentTimeMillis()
//
//
//
//    val endTime = System.currentTimeMillis()
//
//    println("共耗时:" + (endTime - startTime)/1000)
//
//    spark.sparkContext.stop
  }
}
