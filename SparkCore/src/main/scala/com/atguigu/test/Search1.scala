package com.atguigu.test

import org.apache.spark.rdd.RDD

class Search1(query:String) {
  def isMatch(s:String)={
    s.contains(query)
  }

  def getMatch1(rdd:RDD[String])={
    rdd.filter(isMatch)
  }

  def getMatch2(rdd:RDD[String])={
    val temp:String = query
    rdd.filter(x=>x.contains(temp))
  }
}
