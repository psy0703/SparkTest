package com.dgmall.sparktest.kafka

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

/**
  * Redis连接池
  */
object RedisClient {

  private var pool:JedisPool = null

  def makePool(redisHost:String,redisPort:Int,redisTimeOut:Int,
               maxTotal:Int,maxIdle:Int,minIdle:Int): Unit ={
    makePool(redisHost,redisPort,redisTimeOut,maxTotal,maxIdle,minIdle,
      true,false,1000)
  }

  def makePool(redisHost: String, redisPort: Int, redisTimeout: Int,
               maxTotal: Int, maxIdle: Int, minIdle: Int, testOnBorrow: Boolean,
               testOnReturn: Boolean, maxWaitMillis: Long): Unit ={
    if(pool == null){
      val poolConfig = new GenericObjectPoolConfig()
      poolConfig.setMaxTotal(maxTotal)
      poolConfig.setMaxIdle(maxIdle)
      poolConfig.setMinIdle(minIdle)
      poolConfig.setTestOnBorrow(testOnBorrow)
      poolConfig.setTestOnReturn(testOnReturn)
      poolConfig.setMaxWaitMillis(maxWaitMillis)

      pool = new JedisPool(poolConfig,redisHost,redisPort,redisTimeout)

      val hook: Thread = new Thread {
        override def run = pool.destroy()
      }
      sys.addShutdownHook(hook.run)
    }
  }

  def getPool:JedisPool={
    assert(pool != null)
    pool
  }
}
