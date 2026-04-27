package querying.main.stores

import com.redis.RedisClientPool

object RedisStore {

  //val redisPool = new RedisClientPool("0.0.0.0", 6379)
  val redisPool = new RedisClientPool(host = "155.223.25.2", port = 6379, secret = Some("mimic-iii.kv123"))

  def get(database: Int, key: Any) = redisPool.withClient {
    client => {
      client.select(database)
      client.get(key)
    }
  }

  def lrange(database: Int, key: Any, start: Int, end: Int) = redisPool.withClient {
    client => {
      client.select(database)
      client.lrange(key, start, end)
    }
  }

  def zrangeWithScore(database: Int, key: Any) = redisPool.withClient {
    client => {
      client.select(database)
      client.zrangeWithScore(key)
    }
  }

  def keys(database: Int, keyPattern: Any) = redisPool.withClient {
    client => {
      client.select(database)
      client.keys(keyPattern)
    }
  }


}
