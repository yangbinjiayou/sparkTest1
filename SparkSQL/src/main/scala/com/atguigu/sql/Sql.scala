package com.atguigu.sql

import org.apache.hadoop.yarn.webapp.example.HelloWorld.Hello
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object TestSql {
  def main(args: Array[String]): Unit = {
    //1.获取sparksession对象
    val spark: SparkSession = SparkSession
      .builder()
      .appName("TestSql")
      .master("local[*]")
      .getOrCreate()
    //2.导入隐式转换
    import spark.implicits._
    val df: DataFrame = spark.read.json("D:\\software\\sparkTest1\\SparkSQL\\src\\main\\resources\\people.json")
    df.select($"age").show()
    //df.filter($"age">20).show()
    df.createTempView("people")
    //spark.sql("select * from people").show()

    /*//3.测试
    val sc: SparkContext = spark.sparkContext
    val rdd: RDD[Int] = sc.parallelize(Array(1,2,3))
    val df: DataFrame = rdd.map(x => {
      Hello(x)
    }).toDF()
    df.createTempView("hello")
    //spark.read.json("")
    val rdd1: RDD[Row] = df.rdd
    val dataset: Dataset[Hello] = rdd.map(x => {
      Hello(x)
    }).toDS()
    val rdd2  = dataset.rdd

    val ds = df.as[Hello]
    val df2 = ds.toDF()*/
    //rdd1.collect().foreach(println(_))
    /*spark.sql("select * from hello  where id = 1").show()*/
    //关闭对象
    spark.stop()
  }
}
case class Hello(id:Int){

}