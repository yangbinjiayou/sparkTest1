package com.atguigu.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestCustomPartitioner {
  def main(args: Array[String]): Unit = {
    //1.获取sparkcontext对象
    val sparkConf: SparkConf = new SparkConf().setAppName("TestCustomPartitioner").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    //2.创建rdd
    val customPartitioner = new CustomPartitioner(3)
    val rdd: RDD[Int] = sc.parallelize(Array(1,2,3),3)

    val one: RDD[(Int, Int)] = rdd.map((_,1))

    val result1 = one.mapPartitionsWithIndex((i,items) =>items.map((i,_)))

    result1.collect().foreach(println(_))
    //关闭资源
    println("----------------------------")
    val two: RDD[(Int, Int)] = one.partitionBy(customPartitioner)
    val result2: RDD[(Int, (Int, Int))] = two.mapPartitionsWithIndex((i,items)=>items.map((i,_)))

    result2.collect().foreach(println(_))
    sc.stop()
  }
}
