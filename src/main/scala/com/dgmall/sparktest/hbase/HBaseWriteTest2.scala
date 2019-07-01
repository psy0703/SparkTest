package com.dgmall.sparktest.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 通过TableOutputFormat向HBase写数据
  * @Author: Cedaris
  * @Date: 2019/6/20 10:48
  */
object HBaseWriteTest2 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("HBaseWriteTest2")
      .master("local[2]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val tableName = "imooc_course_clickcount"
    val quorum = "psy831"
    val port = "2181"

    val conf: Configuration = HBaseUtils.getHBaseConfiguration(quorum,port)
    conf.set(TableOutputFormat.OUTPUT_TABLE,tableName)

    val jobConf = new JobConf()
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,tableName)

    //写入数据到HBase
    val indata: RDD[String] = sc.makeRDD(Array("20180723_02,10","20180723_03,10","20180818_03,50"))
    val unit: RDD[Array[String]] = indata.map(_.split(","))
    val rdd = indata.map(_.split(",")).map{arr => {
      val put = new Put(Bytes.toBytes(arr(0)))
      put.add(Bytes.toBytes("info"),Bytes.toBytes("clict_count"),Bytes.toBytes(arr(1)))
      (new ImmutableBytesWritable,put)
    }}.saveAsHadoopDataset(jobConf)

    sc.stop()
  }

}
