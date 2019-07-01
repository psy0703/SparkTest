package com.dgmall.sparktest.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Admin, HTable, Put}
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 通过HTable中的Put向HBase写数据
  * @Author: Cedaris
  * @Date: 2019/6/20 10:11
  */
object HBaseWriteTest1 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("HBaseWriteTest1")
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val tableName = "imooc_course_clickcount"
    val quorum = "psy831"
    val port = "2181"


    // 配置相关信息
    val conf: Configuration = HBaseUtils.getHBaseConfiguration(quorum,port)
    conf.set(TableOutputFormat.OUTPUT_TABLE,tableName)

    val admin: Admin = HBaseUtils.getHBaseAdmin(conf,tableName)

    val indataRDD: RDD[String] = sc.makeRDD(Array("002,10","003,10","004,50"))

    indataRDD.foreachPartition(x => {
      val conf: Configuration = HBaseUtils.getHBaseConfiguration(quorum,port)
      conf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
      val table: HTable = HBaseUtils.getTable(conf,tableName)

      x.foreach(y => {
        val arr: Array[String] = y.split(",")
        val key: String = arr(0)
        val value: String = arr(1)

        val put = new Put(Bytes.toBytes(key))
        put.add(Bytes.toBytes("info"),Bytes.toBytes("clict_count"),Bytes
          .toBytes(value))

        table.put(put)
      })
    })

    sc.stop()
  }
}
