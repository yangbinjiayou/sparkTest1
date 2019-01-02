package com.atguigu.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AD {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("ad").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val file: RDD[String] = sc.textFile("d:\\agent.log")

    val proAndAdToOne: RDD[((String, String), Int)] = file.map {
      x => {
        val strings: Array[String] = x.split(" ")
        ((strings(1), strings(4)), 1)
      }
    }
    
    val proAndAdToSum: RDD[((String, String), Int)] = proAndAdToOne.reduceByKey(_+_)
    val ProToAdAndSum: RDD[(String, (String, Int))] = proAndAdToSum.map(x=>(x._1._1,(x._1._2,x._2)))
    val progroup: RDD[(String, Iterable[(String, Int)])] = ProToAdAndSum.groupByKey()
    val top3: RDD[(String, List[(String, Int)])] = progroup.mapValues(x => {
      x.toList.sortWith((x, y) => x._2 > y._2).take(3)
    })
    top3.sortByKey().collect().foreach(println)
    sc.stop()

  }
}
