package com.atguigu.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object TestUDAF {
  def main(args: Array[String]): Unit = {
    //创建sparksession对象
    val spark: SparkSession = SparkSession
      .builder()
      .appName("TestUDAF")
      .master("local[*]")
      .getOrCreate()
    //导入spark对象
    import spark.implicits._
    //
    val df: DataFrame = spark.read.json("D:\\software\\sparkTest1\\SparkSQL\\src\\main\\resources\\people.json")

    df.createTempView("ss")

    spark.sql("select * from ss").show()
    /*val s = new udaf
    spark.udf.register("haha",s)
    spark.sql("select haha(age) from ss").show()*/
    //关闭sparksession
    spark.stop()
  }
}

