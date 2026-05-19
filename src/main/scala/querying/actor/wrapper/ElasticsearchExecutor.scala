package querying.actor.wrapper

import akka.actor.{Actor, ActorLogging, PoisonPill, Props}
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.searches.SearchResponse
import com.sksamuel.elastic4s.{RequestFailure, RequestSuccess}
import org.apache.spark.util.SizeEstimator
import querying.main.stores.ElasticsearchStore
import querying.main.{LatencyLogger, QueryingUtils}
import querying.message.{ExecuteQuery, Result}
import querying.transformation.ElasticsearchTransformer

object ElasticsearchExecutor {
  def props: Props = Props(new ElasticsearchExecutor)
}

class ElasticsearchExecutor extends Actor with ActorLogging {

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
      log.info("Size of the new result message sent start ElasticsearchExecutor end Federator is: [{}] Bytes, and is [{}]", sizeInBytes, QueryingUtils.formatByteValue(sizeInBytes))
      self ! PoisonPill
  }

  protected def executeQuery(query: String, queryId: String): Option[Result] = {
    var result: Option[Result] = None

    // === Experiment-4: Store execution start ===
    val tStoreStart = System.nanoTime()
    val resp = ElasticsearchStore.client.execute {
      search("noteevents").rawQuery(query)
    }.await
    val tStoreEnd = System.nanoTime()

    // === Experiment-4: RDF transformation (transformation to global model) start ===
    val tTransformStart = System.nanoTime()
    resp match {
      case failure: RequestFailure => println("We failed " + failure.error)
      case results: RequestSuccess[SearchResponse] =>
        result = ElasticsearchTransformer.transformToRdfResult(results)
      case results: RequestSuccess[_] => println(results.result)
    }
    val tTransformEnd = System.nanoTime()

    LatencyLogger.logExecutorPhase(
      queryId = queryId,
      store = "elasticsearch",
      storeExecMs = (tStoreEnd - tStoreStart) / 1e6,
      transformMs = (tTransformEnd - tTransformStart) / 1e6
    )
    result
  }

}