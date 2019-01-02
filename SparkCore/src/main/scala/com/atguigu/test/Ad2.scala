package com.atguigu.test

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Ad2 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("xiaoshi").setMaster("local[*]")

    val context = new SparkContext(sparkConf)

    val line: RDD[String] = context.textFile("d:\\agent.log")

    val lines: RDD[((String, String, String), Int)] = line.map(x => {
      val fields: Array[String] = x.split(" ")
      ((new SimpleDateFormat("yyyyMMddhh").format(fields(0).toLong), fields(1), fields(4)), 1)
    })
    val timeProAdToSum: RDD[((String, String, String), Int)] = lines.reduceByKey((x,y)=>x+y)
    val timeProTo: RDD[((String, String), (String, Int))] = timeProAdToSum.map(x=>((x._1._1,x._1._2),(x._1._3,x._2)))
    val timeProGroup: RDD[((String, String), Iterable[(String, Int)])] = timeProTo.groupByKey()
    val results: RDD[((String, String), List[(String, Int)])] = timeProGroup.mapValues(
      x => x.toList.sortWith((x, y) => x._2 > y._2).take(3)
    )
    results.sortByKey().collect().foreach(println(_))


  context.stop()
  }
}
