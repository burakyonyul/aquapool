import com.redis.RedisClientPool
import org.scalatest.{FlatSpec, Matchers}


class RedisConnectionTest extends FlatSpec with Matchers {
  val HOST="155.223.25.3"
  //val HOST="localhost"
  val clients = new RedisClientPool(HOST, 6379)

  "Redis client " should "do the operations to redis database" in {
    // delete store before start
    println(get("query-count").getOrElse("no value for index 'query-count'"))
    println(get("actor-count").getOrElse("no value for index 'actor-count'"))
    //deleteStore
    //println(get("query-count").getOrElse("no value for index 'query-count'"))
    //println(get("actor-count").getOrElse("no value for index 'actor-count'"))
    clients.close
    /*set("my-key", "13")
    println(get("my-key").get)
    incr("my-key")
    println(get("my-key").get)
    val fut = Future {
      incr("my-key")
    }
    fut.onComplete {
      case Success(value) => {
        println(s"Key value: ${value.get}")
      }
      case Failure(e) => {
        println(s"Increment operation is failed because of: ${e.getMessage}")
      }
    }
    val res = Await.result(fut, 1.seconds).get
    println(res)

    rpush("my-list", "25")
    rpush("my-list", "26")
    rpush("my-list", "27")
    rpush("my-list", "25")
    println(rpop("my-list").get)
    println(rpop("my-list").get)
    println(rpop("my-list").get)
    println(rpop("my-list").get)

    //println(get("my-list"))
    */
  }

  private def deleteStore = clients.withClient {
    client => {
      client.flushall
    }
  }

  def set(key: String, value: String) = clients.withClient {
    client => {
      client.set(key, value)
    }
  }

  def incr(key: String) = clients.withClient {
    client => {
      client.incr(key)
    }
  }

  def get(key: String) = clients.withClient {
    client => {
      client.get(key)
    }
  }

  def lpush(key: Any, value: Any) = clients.withClient {
    client => {
      client.lpush(key, value)
      client.llen(key)
    }
  }

  def rpush(key: Any, value: Any) = clients.withClient {
    client => {
      client.rpush(key, value)
      client.llen(key)
    }
  }

  def lpop(key: Any) = clients.withClient {
    client => {
      client.lpop(key, value)
    }
  }

  def rpop(key: Any) = clients.withClient {
    client => {
      client.rpop(key)
    }
  }

}
