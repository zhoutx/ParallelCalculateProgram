package com.epri.dlsc.sbs.calc.test


import java.util.Properties

import com.epri.dlsc.sbs.calc.config.Configuration
import javax.script.{Compilable, ScriptContext, ScriptEngineManager, SimpleBindings}
import org.apache.parquet.format.IntType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Encoder, Row, SparkSession}

import scala.collection.mutable




object SparkReadFromOracle {
  /**
    * args(0) 为partition数量
    */
  def main(args1: Array[String]): Unit = {

    val map = mutable.LinkedHashMap[String, Int]()
    map += ("aaaa"-> 1) += ("1111"->4) += ("ddddd"->3) += ("0000"->4) += ("bbbbb"->1)
    map.-=("ddddd")
    map += ("ddddd"->10)
    map.values.toList.foreach(println(_))

  }


}
