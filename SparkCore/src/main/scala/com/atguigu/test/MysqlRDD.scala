package com.atguigu.test

import java.sql.DriverManager

import com.sun.rowset.internal.Row
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object MysqlRDD {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("MysqlRDD").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://hadoop222:3306/rdd"
    val userName = "root"
    val passWd = "cpmsroot"
    val jdbcRdd = new JdbcRDD[Int](
      sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, userName, passWd)
      },
      "select id from emp where ? <= id and id <= ?",
      2,
      5,
      2,
      rs => rs.getInt(1)
    )
    jdbcRdd.collect().foreach(println(_))

    sc.stop()
  }
}
