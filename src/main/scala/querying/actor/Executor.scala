package querying.actor

import akka.actor.{Actor, ActorLogging}
import akka.cluster.sharding.ShardRegion
import com.hp.hpl.jena.query.QueryExecutionFactory
import org.apache.spark.util.SizeEstimator
import querying.main.MonitoringUtils
import querying.message.{ExecuteServiceClause, Result}

object Executor {
  val extractEntityId: ShardRegion.ExtractEntityId = {
    case esc@ExecuteServiceClause(_, _) => (esc.hashCode.toString, esc)
  }

  private val numberOfShards = 20

  val extractShardId: ShardRegion.ExtractShardId = {
    case esc@ExecuteServiceClause(_, _) => (esc.hashCode % numberOfShards).toString
  }
}

class Executor extends Actor with ActorLogging {

  override def preStart(): Unit = {
    super.preStart
    //MetricStoreUtils.increaseActorCount
    //log.debug("Actor count has been increased")
  }

  override def postStop(): Unit = {
    super.postStop
    //MetricStoreUtils.decreaseActorCount
    //log.debug("Actor count has been decreased")
  }

  override def receive: Receive = {
    case esc@ExecuteServiceClause(query, endpoint) =>
      //log.info("Sender path: {}, self path {}",sender().path,self.path)
      log.debug("Hash Code for Execute SERVICE Clause: [{}], and Query Value: [{}], Endpoint Value: [{}]", esc.hashCode, query, endpoint)
      val result = executeServiceClause(query, endpoint)
      sender ! result
      val sizeInBytes = SizeEstimator.estimate(result)
      log.info("Size of the new result message sent from Executor to Distributor is: [{}] Bytes, and is [{}]",sizeInBytes, MonitoringUtils.formatByteValue(sizeInBytes))
  }

  protected def executeServiceClause(query: String, endpoint: String) = {
    val execution = QueryExecutionFactory.sparqlService(endpoint, query)
    val result = MonitoringUtils.convertRdf2Result(execution.execSelect())
    execution.close()
    Result(result.resultJSON, result.resultVars, endpoint.hashCode)
  }

}