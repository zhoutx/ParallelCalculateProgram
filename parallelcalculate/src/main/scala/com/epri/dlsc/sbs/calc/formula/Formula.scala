package com.epri.dlsc.sbs.calc.formula

import scala.collection.mutable

class Formula(
       val id: String,
       val name: String,
       val calcDataSetId: String,
       val filters: Map[String, String] = Map[String, String](),
       val formulaItems: Seq[FormulaItem] = List[FormulaItem]()
             ) extends Serializable {
  val formulaItemsMap: mutable.HashMap[String, FormulaItem] = mutable.HashMap[String, FormulaItem]()

  def formulaItem(id: String): FormulaItem = {
    if(formulaItems.isEmpty){
      null
    }else{
      if(formulaItemsMap.isEmpty){
        formulaItems.foreach(item => formulaItemsMap.put(item.id, item))
      }
      val item = formulaItemsMap.get(id)
      if(item.isEmpty){
        null
      }else{
        item.get
      }
    }
  }
}
object Formula {
  def apply(
      formulaId: String,
      formulaName: String,
      calcDataSetId: String): Formula = new Formula(formulaId, formulaName, calcDataSetId)
}

//子公式
class FormulaItem(
      val id: String,
      val field: String,
      val fieldName: String,
      val formulaContent: String,
      val formulaText: String) extends Serializable

object FormulaItem {
  def apply(
      id: String,
      field: String,
      fieldName: String,
      formulaContent: String,
      formulaText: String): FormulaItem =
    new FormulaItem(id, field, fieldName, formulaContent, formulaText)
}