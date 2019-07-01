package com.dgmall.sparktest.phoenixTest

/**
  * @Author: Cedaris
  * @Date: 2019/7/1 11:21
  */

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.phoenix.spark._
import org.apache.spark.rdd.RDD

object PhoenixTest {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration
    conf.set("hbase.zookeeper.quorum","psy831:2181,psy832:2181,psy833:2181")

    val sc = new SparkContext("local[*]","phoenix-spark")
    val sqlContext = new SQLContext(sc)

    //Reading Phoenix Tables
    //Load as a DataFrame using the Data Source API
    val df1: DataFrame = sqlContext.load(
      "org.apache.phoenix.spark",
      Map("table" -> "TABLE1", "zkUrl" -> "psy831:2181")
    )
    df1.filter(df1("COL1") === "test_row_1" && df1("ID") === 1L)
      .select(df1("ID"))
      .show()

    //Load as a DataFrame directly using a Configuration object
    val df2: DataFrame = sqlContext.phoenixTableAsDataFrame(
      "TABLE1", Array("ID", "COL1"), conf = conf
    )

    df2.show()

    //Load as an RDD, using a Zookeeper URL
    // Load the columns 'ID' and 'COL1' from TABLE1 as an RDD
    val rdd: RDD[Map[String, AnyRef]] = sc.phoenixTableAsRDD(
      "TABLE1", Seq("ID", "COL1"), zkUrl = Some("psy831:2181")
    )

    rdd.count()

    val firstId = rdd.first()("ID").asInstanceOf[Long]
    val firstCol = rdd.first()("COL1").asInstanceOf[String]


    //Saving Phoenix
    //Saving RDDs
    val dataSet = List((1L, "1", 1), (2L, "2", 2), (3L, "3", 3))

    sc.parallelize(dataSet)
      .saveToPhoenix(
        "OUTPUT_TEST_TABLE",
        Seq("ID","COL1","COL2"),
        zkUrl = Some("psy831:2181")
      )

//    Saving DataFrames
    df1.write
      .format("org.apache.phoenix.spark")
      .mode("overwrite")
      .option("table","OUTPUT_TABLE")//输出表名
      .option("zkUrl","psy831:2181")
      .save()

  }
}
