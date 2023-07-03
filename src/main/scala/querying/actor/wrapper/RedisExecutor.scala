package querying.actor.wrapper

import akka.actor.{Actor, ActorLogging, Props}
import org.apache.spark.util.SizeEstimator
import querying.main.QueryingUtils
import querying.main.stores.RedisStore
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

  /**
   * Query format should be: '[database] [operation_name] [{key1},{key2},{key3},...]'
   */
  protected def executeQuery(query: String): Option[Result] = {

    val keywords = parseQueries(query)
    val database = keywords.head.asInstanceOf[String].toInt
    val operation = keywords(1).asInstanceOf[String]
    val keys: List[String] = keywords.last.asInstanceOf[List[String]]
    var resultMap = Map.empty[String, Option[Any]]

    if (keywords.length != 3) {
      log.debug("No operation is supported for the given query: [{}]", query)
      return None
    }

    for (key <- keys) {
      operation match {
        case "keys" => resultMap += (key -> RedisStore.keys(database, s"""$key"""))
        case "get" => resultMap += (key -> RedisStore.get(database, key))
        case "lrange" => resultMap += (key -> RedisStore.lrange(database, key, 0, 501))
        case "zrangeWithScore" => resultMap += (key -> RedisStore.zrangeWithScore(database, key))
      }
    }
    RedisTransformer.transformToRdfResult(database, operation, resultMap)
  }

  private def parseQueries(query: String): List[Any] = {
    val queryRegex = "(?<=\\[)(.*?)(?=\\])".r
    val matchList: List[String] = queryRegex.findAllIn(query).toList
    val database: String = matchList.head
    val operationName: String = matchList(1)
    val lastMatch: String = matchList.last

    val keywordRegex = "(?<=\\{)(.*?)(?=\\})".r
    val keyList: List[String] = keywordRegex.findAllIn(lastMatch).toList
    List(database, operationName, keyList)
  }

}