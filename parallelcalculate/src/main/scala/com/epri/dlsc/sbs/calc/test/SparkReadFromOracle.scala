package com.epri.dlsc.sbs.calc.test

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._



object SparkReadFromOracle {
  /**
    * args(0) 为partition数量
    */
  def main(args1: Array[String]): Unit = {

    val args = Array("3")

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

    val partitionProp = new Array[String](num)
    for(i <- 0 to (num - 1)){
      partitionProp(i) = "MKT_MONTH = TO_DATE('201701','yyyymm') AND MOD(ORA_HASH(SBS_UNIT_ID,10000)," +num+ ") = " + i
    }

    val spark: SparkSession = SparkSession
      .builder()
      .appName("Spark SQL Read Data From Oracle")
      .master("local[10]")
      .getOrCreate()

    import spark.implicits._

    val connectionProperties = new Properties()
    connectionProperties.put("user", "zjsgbiz")
    connectionProperties.put("password", "zjsgbiz")

    val dataFrame: DataFrame = spark.read.jdbc(
      "jdbc:oracle:thin:@10.6.0.85:1521:dlsc",
      "SE_RESULT_N_M",
      partitionProp,
      connectionProperties)

    val dataFrame2: DataFrame = spark.read.jdbc(
      "jdbc:oracle:thin:@10.6.0.85:1521:dlsc",
      "SE_MONTH_ENERGY",
      partitionProp,
      connectionProperties)

    val startTime = System.currentTimeMillis()
    import org.apache.spark.sql.functions._


    val result = dataFrame2.join(dataFrame, Array("MARKET_ID", "SBS_UNIT_ID"),"left_outer")
        .groupBy(dataFrame2("MARKET_ID"), dataFrame2("MKT_MONTH"), dataFrame2("SBS_UNIT_ID"))
        .agg(sum($"T_ENERGY"),sum($"ENERGY_T"))
    result.cache.show()

//    dataFrame.

//    println(dataFrame2.count())
//    println(result.count())

//    val datasByGroup = v1AndV2.groupByKey()

//    v1AndV2.collect.foreach(row=>{
//      val sbs_unit_id = row._1
//      val coneng = row._2._1.getAs[Number]("ENERGY_T")
//      val neteng = row._2._2.getAs[Number]("T_ENERGY")
//      println(s"$sbs_unit_id||$coneng||$neteng")
//    })


//    val dataFrameMid = dataFrame.union(dataFrame2)
//    println("--------------------->"+ dataFrameMid.count())

//    dataFrame.createOrReplaceTempView("SE_RESULT_N_M")
//
//
//    val oracleDatas: DataFrame = spark.sql("SELECT * FROM SE_RESULT_N_M T")
//
//    val repartitionDS: Dataset[Row] = oracleDatas.repartition(10, $"SBS_UNIT_ID")
//
//    val value: RDD[(String, Iterable[Row])] = repartitionDS.rdd.groupBy(row => row.getAs[String]("SBS_UNIT_ID"))
//    value.foreach(x=>{
//      val k: String = x._1
//      val v: Iterable[Row] = x._2
//      print(k)
//      print("-->")
//      v.foreach(x=>{
//        print(x.getAs[BigDecimal]("ENERGY_T") + "  ")
//      })
//      println()
//    })

    spark.close()
    println("耗时："  + (System.currentTimeMillis() - startTime) + "ms")
    //    val rddRow: RDD[Row] = oracleDatas.rdd.mapPartitionsWithIndex((index, rows)=>{
    //      var acc: Long = 0L
    //      var phyNum = 0L
    //      rows.map(row=>{
    //        acc = acc + 1
    //        if(acc%10 == 0) phyNum = phyNum + 1
    //        val guid: String = s"${index}_${phyNum}_${LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"))}_${acc}"
    //        val condition1: String = row.getAs("CONDITION1")
    //        val condition2: String = row.getAs("CONDITION2")
    //        val condition3: String = row.getAs("CONDITION3")
    //        val eng: java.math.BigDecimal = row.getAs("ENG")
    //        val price: java.math.BigDecimal = row.getAs("PRICE")
    //        val fee: java.math.BigDecimal = row.getAs("FEE")
    //        Row(guid, condition1, condition2, condition3, eng, price, fee)
    //      })
    //    })
    //
    //    //schema
    //    val columns = List(
    //      StructField("GUID", StringType),
    //      StructField("CONDITION1", StringType),
    //      StructField("CONDITION2", StringType),
    //      StructField("CONDITION3", StringType),
    //      StructField("ENG", DecimalType(38,10)),
    //      StructField("PRICE", DecimalType(38,10)),
    //      StructField("FEE", DecimalType(38,10))
    //    )
    //    val frame: DataFrame = spark.createDataFrame(rddRow, StructType(columns))
    //
    //    oracleDatas.write.format("org.apache.phoenix.spark")
    //      .mode(SaveMode.Overwrite)
    //      .option("table", "AAA")
    //      .option("zkUrl", "node1,node2,node3:2181")
    //      .save()
    //

  }

}
