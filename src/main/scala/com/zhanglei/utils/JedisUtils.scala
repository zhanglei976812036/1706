package com.zhanglei.utils

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
  * Created by Administrator on 2018/5/9.
  */
object JedisUtils {
  val config = new JedisPoolConfig()
  val jedisPool = new JedisPool(config,"h3",6379,2)

  def getConnection() = {
    jedisPool.getResource
  }
  //向 redis中写入hash数据类型的数据
  def hset(key:String,field:String,value:String) = {
    val client = getConnection()
    client.hset(key,field,value)
    client.close()
  }
}
