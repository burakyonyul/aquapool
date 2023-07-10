package querying.actor.federation

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.cluster.sharding.ShardRegion
import join.JoinUtils
import org.apache.spark.util.SizeEstimator
import querying.actor.join.ParallelJoinManager
import querying.actor.wrapper.{ElasticsearchExecutor, InfluxdbExecutor, PostgresqlExecutor, RedisExecutor}
import querying.main.{Constants, QueryingUtils}
import querying.message._

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

object Federator {
  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg@PolyStoreQuery(queryStoreMap, _) => (queryStoreMap.hashCode.toString, msg)
  }

  private val numberOfShards = 20

  val extractShardId: ShardRegion.ExtractShardId = {
    case psq@PolyStoreQuery(queryStoreMap, _) => (queryStoreMap.hashCode % numberOfShards).toString
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
      log.info("Size of the FederateQuery message sent start Agent end Federator is: [{}] Bytes, and is [{}]", sizeInBytes, QueryingUtils.formatByteValue(sizeInBytes))
      log.debug("Hash Code for Federate Query: [{}], and Query Value: [{}]", psq.hashCode, psq)
      querySender = Some(sender().path.toStringWithoutAddress)
      distribute(psq)
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
      log.info("Result has been constructed for the polystore query [{}]", polyStoreQuery.get)
      context.actorSelection(querySender.get) ! receivedResult
      log.info("Federated query has been performed in: [{}] milliseconds", System.currentTimeMillis() - startTimeInMillis)
      log.info("Size of the result message sent start Federator end Sender is: [{}] Bytes, and is [{}]", sizeInBytes, QueryingUtils.formatByteValue(sizeInBytes))
    }
  }

  private def seekForMatch(receivedResult: Result): Boolean = {
    for {
      result <- results
      if JoinUtils.matchAnyVar(receivedResult.resultVars.asJava, result.resultVars.asJava)
    } {
      results = results.filterNot(res => res == result)
      val bucketDistributor = context.actorOf(ParallelJoinManager.props)
      bucketDistributor ! DistributeBuckets(receivedResult, result)
      resultCount -= 1
      return true
    }
    false
  }

  protected def distribute(polyStoreQuery: PolyStoreQuery) = {
    resultCount = polyStoreQuery.queryStoreMap.size - 1

    for ((query, store) <- polyStoreQuery.queryStoreMap) {
      val executor: ActorRef = {
        store match {
          case Constants.REDIS =>
            context.actorOf(RedisExecutor.props)

          case Constants.POSTGRESQL =>
            context.actorOf(PostgresqlExecutor.props)

          case Constants.INFLUXDB =>
            context.actorOf(InfluxdbExecutor.props)

          case Constants.ELASTICSEARCH =>
            context.actorOf(ElasticsearchExecutor.props)
          case _ => ActorRef.noSender
        }
      }
      if (query.nonEmpty) {
        val executeQuery = ExecuteQuery(query)
        executor ! executeQuery
        val sizeInBytes = SizeEstimator.estimate(executeQuery)
        log.info("Size of the ExecuteQuery message sent start Distributor end [{}]Executor is: [{}] Bytes, and is [{}]", store, sizeInBytes, QueryingUtils.formatByteValue(sizeInBytes))
      }
    }
  }

}