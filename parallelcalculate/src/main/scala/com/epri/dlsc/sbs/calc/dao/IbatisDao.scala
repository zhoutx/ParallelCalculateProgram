package com.epri.dlsc.sbs.calc.dao

import scala.collection.JavaConversions._


import com.ibatis.common.resources.Resources
import com.ibatis.sqlmap.client.{SqlMapClient, SqlMapClientBuilder}

object IbatisDao {
  val reader = Resources.getResourceAsReader("SqlMapConfig.xml")
  val sqlMapper = SqlMapClientBuilder.buildSqlMapClient(reader)

  def seMonthEnergy(): java.util.List[_] = {
    val starttime = System.currentTimeMillis()
    val list = sqlMapper.openSession().queryForList("test")
    println((System.currentTimeMillis - starttime) + "ms")
    list
  }

}
