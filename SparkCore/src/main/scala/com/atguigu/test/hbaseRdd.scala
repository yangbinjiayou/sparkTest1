package com.atguigu.test

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.{RDD}

object TestHbaseRdd {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TestHbaseRdd").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "hadoop222,hadoop223,hadoop224")
    conf.set(TableInputFormat.INPUT_TABLE, "fruit")

    val hbaseRdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    hbaseRdd.foreach(x => {
      x._2.rawCells().foreach(
        cell => println(Bytes.toString(CellUtil.cloneRow(cell)))
      )
    })
    sc.stop()
  }
}
