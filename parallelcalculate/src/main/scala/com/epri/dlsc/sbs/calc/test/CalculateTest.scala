package com.epri.dlsc.sbs.calc.test

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

object CalculateTest {
  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()

    val spark: SparkSession = SparkSession
      .builder()
      .appName("Spark SQL Read Data From Oracle")
      .getOrCreate()

    val dataFrame1_0: DataFrame = spark.read.format("org.apache.phoenix.spark")
      .options(Map("table"->"TEST_100MILLION",
                   "zkUrl"->"node1,node2,node3:2181"))
      .load()
    dataFrame1_0.createOrReplaceTempView("TEST_100MILLION")
    spark.udf.register("uniqueID", (x: String) => x.substring(0, x.lastIndexOf("_")))
    val dataFrame1 = spark.sql("select uniqueID(GUID) GUID, GUID ID, CONDITION1, CONDITION2, CONDITION3, ENG, PRICE FROM TEST_100MILLION")
//    dataFrame1.rdd.saveAsObjectFile("alluxio://master2:19998/alluxio/dataFrame1")
    val dataFrame2: DataFrame = spark.read.format("org.apache.phoenix.spark")
      .options(Map("table"->"TEST_100MILLION2",
        "zkUrl"->"node1,node2,node3:2181"))
      .load()
//    dataFrame2.rdd.saveAsObjectFile("alluxio://master2:19998/alluxio/dataFrame2")
    val rdd1 = dataFrame1.rdd.map(row =>{
      var value = scala.collection.Map[String, Any]()
      value += (("ENG"->row.getAs[Double]("ENG")))
      value += (("CONDITION1"->row.getAs[String]("CONDITION1")))
      value += (("CONDITION2"->row.getAs[String]("CONDITION2")))
      value += (("CONDITION3"->row.getAs[String]("CONDITION3")))
      value += (("ID"->row.getAs[String]("ID")))
      (row.getAs[String]("GUID"), value)
    })

    val rdd2 = dataFrame2.rdd.map(row =>{
      var value = scala.collection.Map[String, Any]()
      value += (("ENG"->row.getAs[Double]("ENG")))
      value += (("CONDITION1"->row.getAs[String]("CONDITION1")))
      value += (("CONDITION2"->row.getAs[String]("CONDITION2")))
      value += (("CONDITION3"->row.getAs[String]("CONDITION3")))
      (row.getAs[String]("GUID"), value)
    })

    val tmpRDD = rdd1.cogroup(rdd2).map(row =>{
      val datasetList = scala.collection.mutable.ListBuffer[Any]()
      val id: String = row._1
      val itemsGroups = row._2
      itemsGroups.productIterator.foreach(group=>datasetList += group)
      (id, datasetList)
    })

    val calResult = tmpRDD.mapPartitions(rows =>{
      rows.map(row =>{
        val decoms: ListBuffer[scala.collection.Iterable[scala.collection.Map[String, Any]]]
        = row._2.asInstanceOf[ListBuffer[scala.collection.Iterable[scala.collection.Map[String, Any]]]]
        calculateApplication(decoms)
      })
    })

    val resultRDD = calResult.flatMap(row =>{
      row.map(r => r)
    })

    val fields = List(
      StructField("GUID", StringType),
      StructField("CONDITION1", StringType),
      StructField("CONDITION2", StringType),
      StructField("CONDITION3", StringType),
      StructField("ENG", DoubleType))
    val targetSchema = StructType(fields)

    val saveRDD = resultRDD.map(row =>{
      Row(row._1,
        row._2("CONDITION1"),
        row._2("CONDITION2"),
        row._2("CONDITION3"),
        2d
      )})

    val targetResultDF = spark.createDataFrame(saveRDD, targetSchema)
    targetResultDF.write.format("org.apache.phoenix.spark")
      .mode(SaveMode.Overwrite)
      .option("table", "TEST_RESULT2")
      .option("zkUrl", "node1,node2,node3:2181")
      .save()

    println("共耗时:" + (System.currentTimeMillis() - startTime)/1000)

    spark.close()
  }

  def calculateApplication(sources: ListBuffer[scala.collection.Iterable[scala.collection.Map[String, Any]]])
  : TraversableOnce[(String, (scala.collection.Map[String, Any]))] ={
    val results = ListBuffer[(String, scala.collection.Map[String, Any])]()
    val eng = sources(1).toList
    var monthEng: Double = 0
    if(eng.size > 0){
      monthEng = eng(0)("ENG").toString.toDouble
    }
    val decoms = sources(0).toList
    val orderedDecoms = decoms.sortBy(decom => decom("CONDITION1").asInstanceOf[String])(Ordering.String.reverse)
    var index = 0
    orderedDecoms.foreach(decom =>{
      index += 1
      var calVar = decom
      breakable{
        if(index == decoms.size){
          calVar += (("ENG", monthEng))
          break()
          results += ((calVar("ID").toString, calVar))
        }
        var eng: Double = calVar("ENG").toString.toDouble
        if(eng >= monthEng){
          eng = monthEng
          monthEng = 0
        }else{
          monthEng -= eng
        }
        calVar += (("ENG", eng))
      }
      results += ((calVar("ID").toString, calVar))
    })
    results
  }

}
