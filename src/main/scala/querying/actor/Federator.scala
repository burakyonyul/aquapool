package querying.actor

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.cluster.sharding.{ClusterSharding, ShardRegion}
import main.{DirectedQuery, QueryManager, Union}
import org.apache.spark.util.SizeEstimator
import querying.main.MonitoringUtils
import querying.message._

import java.util
import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

object Federator {
  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg@FederateQuery(query, _) => (query.hashCode.toString, msg)
  }

  private val numberOfShards = 20

  val extractShardId: ShardRegion.ExtractShardId = {
    case FederateQuery(query, _) => (query.hashCode % numberOfShards).toString
  }
}

class Federator extends Actor with ActorLogging {

  private var resultCount = 0
  private var results: Vector[Result] = Vector.empty
  private var querySender: Option[String] = None
  private var resultMap: HashMap[Int, Result] = HashMap.empty
  private var federateQuery: Option[FederateQuery] = None
  private var startTimeInMillis = 0L;

  override def preStart(): Unit = {
    super.preStart
    //MetricStoreUtils.increaseActorCount
    //log.info("Actor count has been increased")
  }

  override def postStop(): Unit = {
    super.postStop
    //MetricStoreUtils.decreaseActorCount
    //log.info("Actor count has been decreased")
  }

  override def receive: Receive = {
    case fq@FederateQuery(query, senderPath) =>
      //log.info("Sender path: {}, self path {}",sender().path,self.path)
      startTimeInMillis = System.currentTimeMillis()
      federateQuery = Some(fq)
      //MetricStoreUtils.incrementQueryCount(fq)
      val sizeInBytes = SizeEstimator.estimate(federateQuery)
      log.info("Size of the FederateQuery message sent from Agent to Federator is: [{}] Bytes, and is [{}]", sizeInBytes, MonitoringUtils.formatByteValue(sizeInBytes))
      log.debug("Hash Code for Federate Query: [{}], and Query Value: [{}]", fq.hashCode, query)
      querySender = Some(senderPath)
      federate(query)
    case receivedResult@Result(_, _, _) =>
      // get hash join performer region
      processResult(receivedResult)
    case rc@ResultChange(_, _) =>
      applyChange(rc)
  }

  protected def federate(query: String): Unit = {
    val distributorRegion = ClusterSharding.get(context.system).shardRegion("Distributor")
    federate(query, distributorRegion)
  }

  protected def federate(query: String, federator: ActorRef): Unit = {
    val directedQueries = QueryManager.splitFederatedQuery(query, new util.ArrayList[Union])
    distribute(federator, directedQueries)
  }

  protected def processResult(receivedResult: Result): Unit = {
    if (receivedResult.key != 1) {
      resultMap += (receivedResult.key -> receivedResult)
    }
    val matched = seekForMatch(receivedResult)
    if (!matched)
      results = results :+ receivedResult

    // if query completed print result
    if (resultCount == 0 && results.size == 1) {
      val sizeInBytes = SizeEstimator.estimate(receivedResult)
      log.info("Result has been constructed for the federated query [{}]", federateQuery.get.query)
      context.actorSelection(querySender.get) ! receivedResult
      log.info("Federated query has been performed in: [{}] milliseconds", System.currentTimeMillis() - startTimeInMillis)
      log.info("Size of the result message sent from Federator to Agent is: [{}] Bytes, and is [{}]", sizeInBytes, MonitoringUtils.formatByteValue(sizeInBytes))
    }
  }

  private def seekForMatch(receivedResult: Result): Boolean = {
    for {
      result <- results
      if QueryManager.matchAnyVar(receivedResult.resultVars.asJava, result.resultVars.asJava)
    } {
      results = results.filterNot(res => res == result)
      val bucketDistributor = context.actorOf(ParallelJoinManager.props)
      bucketDistributor ! DistributeBuckets(receivedResult, result)
      resultCount -= 1
      return true
    }
    false
  }

  private def applyChange(resultChange: ResultChange) = {
    resultCount = resultMap.size - 1
    resultMap += (resultChange.result.key -> resultChange.result)
    results = resultMap.values.toVector.filterNot(res => res == resultChange.result)
    startTimeInMillis = resultChange.detectionTime
    self ! resultChange.result
  }

  protected def distribute(distributorRegion: ActorRef, directedQueries: util.List[DirectedQuery]) = {
    resultCount = directedQueries.size - 1
    directedQueries forEach {
      directedQuery => {
        directToDistributor(distributorRegion, directedQuery)
      }
    }
  }

  protected def directToDistributor(distributorRegion: ActorRef, directedQuery: DirectedQuery): Unit = {
    val federateServiceClause = DistributeServiceClause(directedQuery.getQuery, directedQuery.getEndpoints.asScala)
    distributorRegion ! federateServiceClause
    val sizeInBytes = SizeEstimator.estimate(federateServiceClause)
    log.info("Size of the DistributeServiceClause message sent from Federator to Distributor is: [{}] Bytes, and is [{}]", sizeInBytes, MonitoringUtils.formatByteValue(sizeInBytes))
  }

}