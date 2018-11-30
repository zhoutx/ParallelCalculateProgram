package com.epri.dlsc.sbs.calc.test

import com.epri.dlsc.sbs.calc.config.Configuration





object ScalaTest {
  def main(args: Array[String]): Unit = {

//    val dataSetIds = List("269ed58663c7de3d5e8a959fc4e9847b", "d3c71f186f6cfb672870c8edeb584bfa")
//    val scriptParams = Map(
//      "datetime" -> "2017-01-15",
//      "taskid"->"dddddd",
//      "tradeseqid"->"20180115000001")
//    val dataSets = Configuration.queryDataSets("95518", "3", "3", scriptParams, dataSetIds)
//    println(dataSets.size)
//    println(dataSets(0).fields)
//    println(dataSets(0).constraintFields)


test(1,2,3,4,5,6)

  }

  def test(a: Int, b: Int*)={
    println(a)
    b.foreach(println(_))
  }
}

