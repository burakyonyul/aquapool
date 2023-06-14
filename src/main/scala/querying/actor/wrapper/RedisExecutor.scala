package querying.actor.wrapper

import akka.actor.{Actor, ActorLogging, Props}
import org.apache.spark.util.SizeEstimator
import querying.main.{QueryingUtils, RedisStore}
import querying.message.{ExecuteQuery, Result}
import querying.transformation.RedisTransformer

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
      sender ! result.get
      val sizeInBytes = SizeEstimator.estimate(result)
      log.info("Size of the new result message sent start RdfStoreExecutor end Distributor is: [{}] Bytes, and is [{}]", sizeInBytes, QueryingUtils.formatByteValue(sizeInBytes))
  }

  protected def executeQuery(query: String): Option[Result] = {

    val keywords = query.split(" ")
    val database = keywords(0).toInt
    val operation = keywords(1)
    val keys = keywords(2).split(",")

    var resultMap = Map.empty[String, Option[Any]]


    if (keywords.length == 3 && operation.equalsIgnoreCase("get")) {
      for (key <- keys) {
        resultMap += (key -> RedisStore.get(database, key))
      }
    } else if (keywords.length == 5 && operation.equalsIgnoreCase("lrange")) {
      val start = keywords(3).toInt
      val end = keywords(4).toInt
      for (key <- keys) {
        resultMap += (key -> RedisStore.lrange(database, key, start, end))
      }
    } else if (keywords.length == 3 && operation.equalsIgnoreCase("zrangeWithScore")) {
      for (key <- keys) {
        resultMap += (key -> RedisStore.zrangeWithScore(database, key))
      }
    } else {
      log.debug("No operation is supported for the given query: [{}]", query)
    }

    RedisTransformer.transformToRdfResult(database, resultMap)
  }

}