package querying.main

import com.redis.RedisClientPool

object MetricStore {

  val redisPool = new RedisClientPool("155.223.25.4", 6379)

  def set(key: Any, value: Any) = redisPool.withClient {
    client => {
      client.set(key, value)
    }
  }

  def incr(key: Any) = redisPool.withClient {
    client => {
      client.incr(key)
    }
  }

  def decr(key: Any) = redisPool.withClient {
    client => {
      client.decr(key)
    }
  }

  def get(key: Any) = redisPool.withClient {
    client => {
      client.get(key)
    }
  }

  def lpush(key: Any, value: Any) = redisPool.withClient {
    client => {
      client.lpush(key, value)
      client.llen(key)
    }
  }

  def rpush(key: Any, value: Any) = redisPool.withClient {
    client => {
      client.rpush(key, value)
      client.llen(key)
    }
  }

  def deleteStore = redisPool.withClient {
    client => {
      client.flushall
    }
  }

}
