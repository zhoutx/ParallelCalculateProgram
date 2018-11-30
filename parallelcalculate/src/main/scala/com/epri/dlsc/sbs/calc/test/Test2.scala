package com.epri.dlsc.sbs.calc.test

import java.sql.ResultSet

import org.apache.spark.{SparkConf, SparkContext}

object Test2 {
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val spark = new SparkContext(conf)
    val textRDD = spark.textFile("alluxio://master2:19998/test")
    spark.setCheckpointDir("alluxio://master2:19998/sparkCheckPoint")
    textRDD.checkpoint()
    textRDD.foreach(println(_))
    System.out.println("耗时："+ (System.currentTimeMillis() - startTime) / 1000 + "s")
    spark.stop()

  }
}
