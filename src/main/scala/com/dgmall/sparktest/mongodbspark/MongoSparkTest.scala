package com.dgmall.sparktest.mongodbspark

import com.mongodb.spark.MongoSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * Spark 操作 MongoDB
  * @Author: Cedaris
  * @Date: 2019/6/19 9:59
  */
object MongoSparkTest {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("MongoSpark")
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://192.168.31.136/testDB.testCollection") // 指定mongodb输入
      .config("spark.mongodb.output.uri", "mongodb://192.168.31.136/testDB.testCollection") // 指定mongodb输出
      .getOrCreate()

    // 设置log级别
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._
    import scala.collection.JavaConverters._
    import org.bson.Document

    val valueRdd: RDD[(String, Int)] = spark.sparkContext.parallelize((1 to 10)
      .map(
      i => ("value", i)
    ))

    val df: DataFrame = MongoSpark.load(spark)



  }

}
