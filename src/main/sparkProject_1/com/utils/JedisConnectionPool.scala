package com.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConnectionPool {

  val jedisConf = new JedisPoolConfig()

  jedisConf.setMaxTotal(20)
  jedisConf.setMaxIdle(10)

  val jedisPool = new JedisPool(jedisConf,"NODE03")

  def getConnection():Jedis={
    jedisPool.getResource
  }

}
