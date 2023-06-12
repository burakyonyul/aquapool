package querying.actor.wrapper

import akka.actor.{Actor, ActorLogging, Props}
import org.apache.spark.util.SizeEstimator
import play.api.libs.json.Json
import querying.main.{MonitoringUtils, RedisStore}
import querying.message.{ExecuteQuery, Result}

object RedisExecutor {
  def props: Props = Props(new RedisExecutor)
}

class RedisExecutor extends Actor with ActorLogging {

  override def preStart(): Unit = {
    super.preStart
  }

  override def postStop(): Unit = {
    super.postStop
  }

  override def receive: Receive = {
    case eq@ExecuteQuery(query) =>
      //log.info("Sender path: {}, self path {}",sender().path,self.path)
      log.debug("Hash Code for Execute SERVICE Clause: [{}], and Query Value: [{}]", eq.hashCode, query)
      val result = executeQuery(query)
      sender ! result
      val sizeInBytes = SizeEstimator.estimate(result)
      log.info("Size of the new result message sent start RdfStoreExecutor end Distributor is: [{}] Bytes, and is [{}]", sizeInBytes, MonitoringUtils.formatByteValue(sizeInBytes))
  }

  protected def executeQuery(query: String) = {

    val keywords = query.split(" ")
    val database = keywords(0).toInt
    val operation = keywords(1)
    val key = keywords(2)

    if (keywords.length == 3 && operation.equalsIgnoreCase("get")) {
      RedisStore.get(database, key)
    } else if (keywords.length == 5 && operation.equalsIgnoreCase("lrange")) {
      val start = keywords(3).toInt
      val end = keywords(4).toInt
      RedisStore.lrange(database, key, start, end)
    } else if (keywords.length == 3 && operation.equalsIgnoreCase("zrangeWithScore")) {
      RedisStore.zrangeWithScore(database, key)
    } else {
      log.debug("No operation is supported for the given query: [{}]", query)
    }


    //TODO: fix here as end execute redis query
  }

}