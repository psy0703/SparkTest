package com.dgmall.sparktest.redis

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import redis.clients.jedis.{Jedis, JedisShardInfo}

/**
  * 广告点击数实时统计：Spark StructuredStreaming + Redis Streams
  * @Author: Cedaris
  * @Date: 2019/7/3 9:38
  */
/*
在StructuredStreaming中把数据处理步骤分成3个子步骤:
从Redis Stream读取、处理数据。
存储数据到Redis。
运行StructuredStreaming程序。
 */
object SparkRedis {

  val redisHost = ""
  val redisPort = ""
  val redisPassword = ""
  val redisTableName = ""

  def main(args: Array[String]): Unit = {
    //创建一个SparkSession并带上Redis的连接信息
    val spark: SparkSession = SparkSession
      .builder()
      .appName("StructuredStreaming on Redis")
      .config("spark.redis.host", redisHost)
      .config("spark.redis.port", redisPort)
      .config("spark.redis.auth", redisPassword)
      .master("local[*]")
      .getOrCreate()

//在Spark中构建schema，我们给流数据命名为“clicks”，并且需要设置参数“stream.kes”的值为“clicks”。由于Redis Stream中的数据包含两个字段：“asset”和“cost”，所以我们要创建StructType映射这两个字段
    val clicks: DataFrame = spark.readStream
      .format("redis")
      .option("stream.keys", redisTableName)
      .schema(StructType(Array(
        StructField("asset", StringType),
        StructField("cost", LongType)
      )))
      .load()
    //统计下每个asset的点击次数，可以创建一个DataFrames根据asset汇聚数据
    val bypass: DataFrame = clicks.groupBy("asset").count()
    //启动StructuredStreaming
    val clickForeachWriter = new ClickForeachWriter(redisHost,redisPort,redisPassword)
    val query: StreamingQuery = bypass
      .writeStream
      .outputMode("update")
      .foreach(clickForeachWriter)
      .start()

  }

}

/**
  * 自定义的ClickForeachWriter向Redis写数据。
  * ClickForeachWriter继承自FroeachWriter，使用Redis的Java客户端Jedis连接到Redis。
  * @param redisHost
  * @param redisPort
  * @param redisPassword
  */
class ClickForeachWriter(redisHost:String,redisPort:String,
                         redisPassword:String) extends  ForeachWriter[Row]{

  var jedis : Jedis = _

  def connect() = {
    val shardInfo = new JedisShardInfo(redisHost,redisPort.toInt)
    shardInfo.setPassword(redisPassword)
    jedis = new Jedis(shardInfo)
  }

  override def open(partitionId: Long, version: Long): Boolean = {
    true
  }

  override def process(value: Row): Unit = {
    val asset: String = value.getString(0) //获取广告ID
    val count: Long = value.getLong(1)     //获取点击数
    if(jedis == null){
      connect()
    }
    jedis.hset("click: " + asset,"asset",asset)
    jedis.hset("click: " + asset,"count",count.toString)
    jedis.expire("click: " + asset,300)
  }

  override def close(errorOrNull: Throwable): Unit ={}
}

/*
--class com.aliyun.spark.redis.StructuredStremingWithRedisStream
--jars /spark_on_redis/ali-spark-redis-2.3.1-SNAPSHOT_2.3.2-1.0-SNAPSHOT.jar,/spark_on_redis/commons-pool2-2.0.jar,/spark_on_redis/jedis-3.0.0-20181113.105826-9.jar
--driver-memory 1G
--driver-cores 1
--executor-cores 1
--executor-memory 2G
--num-executors 1
--name spark_on_polardb
/spark_on_redis/structuredstreaming-0.0.1-SNAPSHOT.jar
xxx1 6379 xxx2 clicks


参数说明：
xxx1： Redis的内网连接地址（host）。
6379：Redis的端口号（port）。
xxx2： Redis的登陆密码。
clicks： Redis的Stream名称
  */
/*
CREATE TABLE IF NOT EXISTS clicks(asset STRING, count INT)
USING org.apache.spark.sql.redis
OPTIONS (
'host' 'xxx1',
'port' '6379',
'auth' 'xxx2',
'table' 'click'
)

参数说明：
xxx1： Redis的内网连接地址（host）。
6379：Redis的端口号（port）。
xxx2： Redis的登陆密码。
click： Redis的Hash 表名称。
 */

