package com.epri.dlsc.sbs.calc.test

import java.util.UUID

import org.apache.phoenix.spark._

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}



object SparkReadFromHBtoHB {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("Spark SQL Read Data From Oracle")
      .master("local[25]")
      .getOrCreate()

    spark.udf.register("uniqueID", (x: String) => x.substring(0, x.lastIndexOf("_")))

    val startTime = System.currentTimeMillis()

//    val dataFrame: DataFrame = spark.read.format("org.apache.phoenix.spark")
//                                .options(Map("table"->"TEST_100MILLION",
//                                             "zkUrl"->"node1,node2,node3:2181"))
//                                .load()
//    dataFrame.show()
    val dataFrame: DataFrame = spark.sqlContext.phoenixTableAsDataFrame("TEST_100MILLION",
                                              columns = List(),
//                                              predicate = Option("GUID LIKE '0_10028%'"),
                                              zkUrl = Option("node1,node2,node3:2181"))
    dataFrame.createOrReplaceTempView("TEST_100MILLION")
    val dataDF = spark.sql("SELECT DISTINCT uniqueID(GUID) GUID FROM TEST_100MILLION")

    val newRDD = dataDF.rdd.map(row =>{
      Row(row.getAs[String]("GUID"), UUID.randomUUID().toString, UUID.randomUUID().toString, UUID.randomUUID().toString, 1000L)
    })

    val schema = StructType(Array(StructField("GUID", StringType),
                              StructField("CONDITION1", StringType),
                              StructField("CONDITION2", StringType),
                              StructField("CONDITION3", StringType),
                              StructField("ENG", LongType)))

//    val structFields = dataDF.schema.fields.map(field =>StructField(field.name, StringType))
//    val schema = StructType(structFields)
//    val rowRDD: RDD[Row] = dataDF.rdd.map(row => {
//      var i = -1
//      val colsData = structFields.map(field =>{
//        i += 1
//        if(row.get(i) != null) {
//          row.get(i).toString
//        }
//        else{
//          null
//        }
//      })
//      CustomRow(colsData)
//    })

    val df = spark.createDataFrame(newRDD, schema)

    df.write.mode(SaveMode.Overwrite).format("org.apache.phoenix.spark")
                                .options(Map("table"->"TEST_100MILLION2",
                                             "zkUrl"->"node1,node2,node3:2181"))
                                .save()
    println("共耗时:" + (System.currentTimeMillis() - startTime)/1000)

    spark.close()
  }

}
