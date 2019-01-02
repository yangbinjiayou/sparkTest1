package com.atguigu.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object CreateDF {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("CreateDF")
      .getOrCreate()
    import spark.implicits._
    /*val df: DataFrame = spark.read.json("D:\\software\\" +
      "sparkTest1\\SparkSQL\\src\\main\\resources\\people.json")
    df.show()*/
    val sc = spark.sparkContext

    val rdd = sc.parallelize(Array(1,2,3))

    val rddrow: RDD[Row] = rdd.map(x=>Row(x))


    spark.stop()
  }
}
