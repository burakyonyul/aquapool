package querying.actor.wrapper

import akka.actor.{Actor, ActorLogging, PoisonPill, Props}
import org.apache.spark.util.SizeEstimator
import querying.main.stores.RedisStore
import querying.main.{LatencyLogger, QueryingUtils}
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
    case eq@ExecuteQuery(query, queryId) =>
      log.debug("Hash Code for Execute SERVICE Clause: [{}], and Query Value: [{}]", eq.hashCode, query)
      val result = executeQuery(query, queryId)
      sender ! result.get
      val sizeInBytes = SizeEstimator.estimate(result)
      log.info("Size of the new result message sent start RedisExecutor end Federator is: [{}] Bytes, and is [{}]", sizeInBytes, QueryingUtils.formatByteValue(sizeInBytes))
      self ! PoisonPill
  }

  /**
   * Query format should be: '[database] [operation_name] [{key1},{key2},{key3},...]'
   */
  protected def executeQuery(query: String, queryId: String): Option[Result] = {

    val keywords = parseQueries(query)
    val database = keywords.head.asInstanceOf[String].toInt
    val operation = keywords(1).asInstanceOf[String]
    val keys: List[String] = keywords.last.asInstanceOf[List[String]]
    var resultMap = Map.empty[String, Option[Any]]

    if (keywords.length != 3) {
      log.debug("No operation is supported for the given query: [{}]", query)
      return None
    }

    // === Deney-4: Store execution start ===
    val tStoreStart = System.nanoTime()

    for (key <- keys) {
      operation match {
        case "reverselrange" => resultMap += (key -> RedisStore.lrange(database, key, 0, -1))
        case "keys" => resultMap += (key -> RedisStore.keys(database, s"""$key"""))
        case "get" => resultMap += (key -> RedisStore.get(database, key))
        case "lrange" => resultMap += (key -> RedisStore.lrange(database, key, 0, -1))
        case "zrangeWithScore" => resultMap += (key -> RedisStore.zrangeWithScore(database, key))
      }
    }

    val tStoreEnd = System.nanoTime()
    // === Deney-4: RDF transformation (global model'e dönüşüm) start ===
    val tTransformStart = System.nanoTime()

    val result = RedisTransformer.transformToRdfResult(database, operation, resultMap)

    val tTransformEnd = System.nanoTime()
    LatencyLogger.logExecutorPhase(
      queryId = queryId,
      store = "redis",
      storeExecMs = (tStoreEnd - tStoreStart) / 1e6,
      transformMs = (tTransformEnd - tTransformStart) / 1e6
    )
    result
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