package querying.actor

import akka.actor.{Actor, ActorLogging}
import akka.cluster.sharding.ShardRegion
import main.QueryManager
import org.apache.spark.util.SizeEstimator
import querying.main.MonitoringUtils
import querying.message.Store.Store
import querying.message._

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

object Federator {
  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg@PolyStoreQuery(hashMap: HashMap[String, Seq[Store]], _) => (hashMap.hashCode.toString, msg)
  }

  private val numberOfShards = 20

  val extractShardId: ShardRegion.ExtractShardId = {
    case PolyStoreQuery(hashMap, _) => (hashMap.hashCode % numberOfShards).toString
  }
}

class Federator extends Actor with ActorLogging {

  private var resultCount = 0
  private var results: Vector[Result] = Vector.empty
  private var querySender: Option[String] = None
  private var resultMap: HashMap[Int, Result] = HashMap.empty
  private var polyStoreQuery: Option[PolyStoreQuery] = None
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
    case psq@PolyStoreQuery(queryStoreMap, senderPath) =>
      //log.info("Sender path: {}, self path {}",sender().path,self.path)
      startTimeInMillis = System.currentTimeMillis()
      polyStoreQuery = Some(psq)
      //MetricStoreUtils.incrementQueryCount(psq)
      val sizeInBytes = SizeEstimator.estimate(polyStoreQuery)
      log.info("Size of the FederateQuery message sent from Agent to Federator is: [{}] Bytes, and is [{}]", sizeInBytes, MonitoringUtils.formatByteValue(sizeInBytes))
      log.debug("Hash Code for Federate Query: [{}], and Query Value: [{}]", psq.hashCode, queryStoreMap)
      querySender = Some(senderPath)
      distribute(queryStoreMap)
    case receivedResult@Result(_, _, _) =>
      // get hash join performer region
      processResult(receivedResult)
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
      log.info("Result has been constructed for the polystore query [{}]", polyStoreQuery.get.queryStoreMap)
      context.actorSelection(querySender.get) ! receivedResult
      log.info("Federated query has been performed in: [{}] milliseconds", System.currentTimeMillis() - startTimeInMillis)
      log.info("Size of the result message sent from Federator to Sender is: [{}] Bytes, and is [{}]", sizeInBytes, MonitoringUtils.formatByteValue(sizeInBytes))
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

  protected def distribute(queryStoreMap: HashMap[String, Seq[Store]]) = {
    resultCount = queryStoreMap.keys.size - 1
    for ((query, storeList) <- queryStoreMap) {
      directToDistributor(query, storeList)
    }
  }

  protected def directToDistributor(query: String, storeList: Seq[Store]): Unit = {
    val distributeQuery = DistributeQuery(query, storeList)
    val distributor = context.actorOf(Distributor.props)
    distributor ! distributeQuery
    val sizeInBytes = SizeEstimator.estimate(distributeQuery)
    log.info("Size of the DistributeQuery message sent from Federator to Distributor is: [{}] Bytes, and is [{}]", sizeInBytes, MonitoringUtils.formatByteValue(sizeInBytes))
  }

}