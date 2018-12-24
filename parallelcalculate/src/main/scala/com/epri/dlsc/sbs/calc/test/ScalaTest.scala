package com.epri.dlsc.sbs.calc.test

import com.epri.dlsc.sbs.calc.service.{ScriptParams, SettleSession, SourceTarget}

object ScalaTest {
  def main(args: Array[String]): Unit = {

    val start = System.currentTimeMillis()

//    val dataSetIds = List("269ed58663c7de3d5e8a959fc4e9847b", "d3c71f186f6cfb672870c8edeb584bfa", "safsdafasd")
//    val scriptParams = Map(
//      "datetime" -> "2017-01-15",
//      "taskid"->"dddddd",
//      "tradeseqid"->"20180115000001")
//    val dataSets = Configuration.queryDataSets("95518", "3", "3", scriptParams, dataSetIds)
//    println(dataSets.size)
//    println(dataSets(0).fields)
//    println(dataSets(0).constraintFields)

    val ids = Array("3dd981efb1d0046b5da13d7777f9ddb2",
                    "9b98140e647925719b89f41171682dfb",
                    "31de374be2a7f0e1c8ae5fc504aed3ca",
                    "b84321c805597b6c1cc792bee12f2963",
                    "815b2c8ba14641f2ea63e01a3d1167ab",
                    "9b98140e647925719b89f41171682dfb")

    val ids2 = Array("33786f06454898b13f57389387afbb1",
        "dc115c669ab7f83e18c8e83084ef9d70",
        "29f85eba1f0d3efcf2ac80eaf6b21766",
        "8741626feeb868dbb00f89ac9d0f9b6f",
        "107fa36778fbef446475b5e3b79126ce",
        "31de374be2a7f0e1c8ae5fc504aed3ca")

//    val list = Configuration.queryFormuals("95518", ids)
//    list.foreach(x=>println(x.name))

    val params = ScriptParams.add("datetime", "2018-01-15")
                  .add("taskid", "dddddd")
                  .add("tradeseqid", "20180115000001")
        .add("starttime", "2018-01-15 00:00:00")
        .add("endtime", "2018-01-15 23:45:00")
    SettleSession.getOrCreate
    .config("95518", SourceTarget("3", "3"),  params,  ids2)
      .submit()

    /**
      * "33786f06454898b13f57389387afbb1",
      * "dc115c669ab7f83e18c8e83084ef9d70",
      * "29f85eba1f0d3efcf2ac80eaf6b21766",
      * "8741626feeb868dbb00f89ac9d0f9b6f",
      * "107fa36778fbef446475b5e3b79126ce",
      * "31de374be2a7f0e1c8ae5fc504aed3ca"
      */

    println((System.currentTimeMillis() - start)/1000 + "s")
  }




}
