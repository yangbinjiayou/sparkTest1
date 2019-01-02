package com.atguigu.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Search {
  def main(args: Array[String]): Unit = {
    val search = new Search1("a")
    /*val bool: Boolean = search.isMatch("yang")
    println(bool)*/
    val sparkConf = new SparkConf().setAppName("Search").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val value: RDD[String] = sc.parallelize(Array("yang","bin"))

    /*val unit: RDD[String] = search.getMatch1(value)
    unit.collect().foreach(println)*/

    search.getMatch2(value).collect().foreach(println(_))

    sc.stop()
  }
}
