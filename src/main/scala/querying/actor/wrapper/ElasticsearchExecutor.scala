package querying.actor.wrapper

import akka.actor.{Actor, ActorLogging, Props}
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.searches.SearchResponse
import com.sksamuel.elastic4s.{RequestFailure, RequestSuccess}
import org.apache.spark.util.SizeEstimator
import querying.main.QueryingUtils
import querying.main.stores.ElasticsearchStore
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
    case eq@ExecuteQuery(query) =>
      //log.info("Sender path: {}, self path {}",sender().path,self.path)
      log.debug("Hash Code for Execute SERVICE Clause: [{}], and Query Value: [{}]", eq.hashCode, query)
      val result = executeQuery(query)
      sender ! result.get
      val sizeInBytes = SizeEstimator.estimate(result)
      log.info("Size of the new result message sent start RdfStoreExecutor end Distributor is: [{}] Bytes, and is [{}]", sizeInBytes, QueryingUtils.formatByteValue(sizeInBytes))
  }

  protected def executeQuery(query: String) = {
    var result: Option[Result] = None
    val resp = ElasticsearchStore.client.execute {
      search("noteevents").rawQuery(query)
    }.await

    resp match {
      case failure: RequestFailure => println("We failed " + failure.error)
      case results: RequestSuccess[SearchResponse] =>
        result = ElasticsearchTransformer.transformToRdfResult(results)
      case results: RequestSuccess[_] => println(results.result)
    }
    result
  }

}