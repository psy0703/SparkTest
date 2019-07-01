package com.dgmall.sparktest.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Admin, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
/**
  * 从HBase读取数据
  *
  * @Author: Cedaris
  * @Date: 2019/6/19 11:24
  */
object HBaseReadTest {
  def main(args: Array[String]): Unit = {
//    readHbase("t_user","psy831","2181")
    val tableName = "t_user"
    val conf: Configuration = HBaseUtils.getHBaseConfiguration("psy831","2181")
    val admin: Admin = HBaseUtils.getHBaseAdmin(conf,tableName)
    HBaseUtils.getAllData(tableName,admin)
    HBaseUtils.close(admin)
  }

  def readHbase(tableName:String,quorum:String ,port:String): Unit ={
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Hbasereader")
      .master("spark://psy831:7077")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    //配置相关信息
    val conf: Configuration = HBaseUtils.getHBaseConfiguration(quorum,port)
    conf.set(TableInputFormat.INPUT_TABLE,tableName)

    //HBase 数据转成RDD
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]).cache()
    //操作RDD
    val data: RDD[(String, String)] = hbaseRDD.map(x => {
      val result = x._2
      val key: String = Bytes.toString(result.getRow)
      val value: String = Bytes.toString(result.getValue("user".getBytes, "username".getBytes))
      (key, value)
    })
    data.foreach(println)

    sc.stop()
  }
}
