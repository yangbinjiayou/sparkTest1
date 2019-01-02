package com.atguigu.sql

import org.apache.spark.sql.SparkSession

object SparkHive {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkHive")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    spark.sql("show tables").show()

    spark.stop()

  }
}
