package com.atguigu.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //创建SparkConfig
    val sparkConf = new SparkConf().setAppName("wordCount")
    //创建SparkContext对象
    val sc = new SparkContext(sparkConf)
    //读取一个文件
    val line: RDD[String] = sc.textFile(args(0))
    //压平
    val words: RDD[String] = line.flatMap(_.split(" "))

    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))

    val wordAndCount: RDD[(String, Int)] = wordAndOne.reduceByKey(_ + _)

    wordAndCount.saveAsTextFile(args(1))

    sc.stop()
  }
}
