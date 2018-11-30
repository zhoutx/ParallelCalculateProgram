package com.epri.dlsc.sbs.calc.test

import javax.script._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import util.control.Breaks._


object SparkReadFromJson {
  def main(args: Array[String]): Unit = {




    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Read Json")
      .getOrCreate()

    /**
      * 1、根据公式内容加载数据源
      *   加载数据源需按顺序加载，从公式“=”号左边往右边顺序
      *   val sourceDF: DataFrame = spark.sqlContext.phoenixTableAsDataFrame(
                                                                "TEST_100MILLION",
                                                                columns = List(),
                                                                predicate = Option("GUID LIKE '0_10028%'"),
                                                                zkUrl = Option("node1,node2,node3:2181"))
      *   target为计算目标，list(0 to ...) 为计算源数据
      */

      val dataSetRelation = scala.collection.mutable.Map[String, RDD[(String, collection.Map[String, Any])]]()

      var target = spark.read.json("d:/ContractEnergy.json").repartition(3)

      var source = List(spark.read.json("d:/MonthEnergy.json").repartition(3))

    /**
      * 2、分别对这些DataFrame按公式中的过滤项进行过滤，并形成新的RDD
      *
      *
      *
     */
//    target.filter(row => row.get)

    /** 3、然后以匹配条件作为key，形成RDD1[(K,V)],RDD2[(K,V)],...  */
    val contractEnergyKV: RDD[(String, scala.collection.Map[String, Any])] = target.rdd.map(row=>{
      (row.getAs[String]("generatorID"),scala.collection.Map("eng"->row.getAs[Long]("eng"),"contractID"->row.getAs[String]("contractID")))
    })
    dataSetRelation += (("0", contractEnergyKV))
    val monthEnergyKV: RDD[(String, scala.collection.Map[String, Any])] = source(0).rdd.map(row=>{
      println("88888888888888888888888888888888|")
      (row.getAs[String]("generatorID"),scala.collection.Map("eng"->row.getAs("eng")))
    })
    dataSetRelation += (("1", monthEnergyKV))

    /** 4、使用这些RDD分别与“=”左边的数据进行cogroup数据分组合并
    *   cogroup操作前需要先将“=”左边的数据分别以这些RDD的K作为Key，形成RDD0[(K,V)] V只要保存唯一约束的属性值就可以了
    *   RDD0[(K,V1)].cogroup(DD1[(K,V2)]) ---> RDD3[(K,(CompactBuffer(...),CompactBuffer(...)))]
    *   RDD0[(K,V1)].cogroup(DD2[(K,V2)]) ---> RDD4[(K,(CompactBuffer(...),CompactBuffer(...)))]
    *   ...
    */
    val tmpRDD = dataSetRelation("0").cogroup(dataSetRelation("1")).map(row =>{
      val datasetList = scala.collection.mutable.ListBuffer[Any]()
      val id: String = row._1
      val itemsGroups = row._2
      itemsGroups.productIterator.foreach(group=>datasetList += group)
      (id, datasetList)
    })
    /** 5、整理这些RDD，最后形成如下格式：
    *   RDD5[(K,CompactBuffer(...))]
    *   RDD6[(K,CompactBuffer(...))]
    *   ...
    *   这里K为"="左边计算源的唯一约束组合Key
    *6、将“=”左边的RDD处理成RDD7[(K,V)]，其中K为自身的唯一约束组合
    *7、最后将这些RDD挨个cogroup，形成RDD8[(K,(CompactBuffer(...),CompactBuffer(...),CompactBuffer(...)))]
    *
    */

    val value: RDD[TraversableOnce[(String, collection.Map[String, Any])]] = tmpRDD.mapPartitions((iterator: Iterator[(String, ListBuffer[Any])]) => {
      iterator.map(row => {
        val generatorID: String = row._1
        val decoms: ListBuffer[Iterable[collection.Map[String, Any]]]
        = row._2.asInstanceOf[ListBuffer[Iterable[collection.Map[String, Any]]]]
        calculateApplication(generatorID, decoms)
      })
    })

    val result = value.flatMap(row =>{
      row.map(r => r)
    })

    dataSetRelation += (("2", result))

    val a = result.map(x=>{
      (x._1 + "0000", x._2)
    })

    a.reduce((a, b) =>{
      ("1", scala.collection.Map[String, Any]("1"->"1"))
    })

    //结束
    spark.close()
  }

  def calculateApplication(id: String, sources: ListBuffer[scala.collection.Iterable[scala.collection.Map[String, Any]]])
  : TraversableOnce[(String, (scala.collection.Map[String, Any]))] ={
    val results = ListBuffer[(String, scala.collection.Map[String, Any])]()
    val list = sources(1).toList
    var monthEng: Long = 0;
    if(list.size > 0){
      monthEng = list(0)("eng").toString.toLong
    }
    val decoms = sources(0).toList
    val orderedDecoms = decoms.sortBy(decom => decom("contractID").asInstanceOf[String])(Ordering.String.reverse)
    var index = 0
    orderedDecoms.foreach(decom =>{
      index += 1
      var calVar = decom
      breakable{
        if(index == decoms.size){
          calVar += (("eng", monthEng))
          break()
          results += ((id, calVar))
        }
        var eng: Long = calVar("eng").toString.toLong
        if(eng >= monthEng){
          eng = monthEng
          monthEng = 0
        }else{
          monthEng -= eng
        }
        calVar += (("eng", eng))
      }
      results += ((id, calVar))
    })
    results
  }


  private var compilable: Compilable = _
  def compileScript(script: String): CompiledScript = {
    if (compilable == null) {
      val engine = new ScriptEngineManager().getEngineByName("javascript")
      compilable = engine.asInstanceOf[Compilable]
    }
    compilable.compile(script)
  }

}