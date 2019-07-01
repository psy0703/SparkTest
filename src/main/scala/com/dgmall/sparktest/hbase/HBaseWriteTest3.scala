package com.dgmall.sparktest.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * 通过bulkload向HBase写数据
  * 思路就是将数据RDD先生成HFiles，
  * 然后通过org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles将事先生成Hfiles批量导入到Hbase中
  * @Author: Cedaris
  * @Date: 2019/6/20 11:07
  */
object HBaseWriteTest3 {

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

    val table: HTable = HBaseUtils.getTable(conf,tableName)
    val job: Job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputKeyClass(classOf[KeyValue])
    // inputRDD data
    val indataRDD = sc.makeRDD(Array("20180723_02,13","20180723_03,13","20180818_03,13"))
    val rdd = indataRDD.map(x => {
      val arr: Array[String] = x.split(",")
      val kv = new KeyValue(Bytes.toBytes(arr(0)), "info".getBytes, "click_count"
        .getBytes, Bytes.toBytes(arr(1)))
      (new ImmutableBytesWritable(Bytes.toBytes(arr(0))),kv)
    }
    )

    //保存Hfile  to HDFS
    rdd.saveAsNewAPIHadoopFile("hdfs://psy831:9000/tmp/hbase",
      classOf[ImmutableBytesWritable],classOf[KeyValue],
      classOf[HFileOutputFormat2],conf)

    //BUlk 写Hfile 到HBase
    val bulkLoader = new LoadIncrementalHFiles(conf)
    bulkLoader.doBulkLoad(new Path("hdfs://psy831:9000/tmp/hbase"),table)
  }
}
