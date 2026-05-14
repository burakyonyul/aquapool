package querying.actor.federation

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill}
import akka.cluster.sharding.ShardRegion
import join.JoinUtils
import org.apache.spark.util.SizeEstimator
import querying.actor.join.ParallelJoinManager
import querying.actor.wrapper.{ElasticsearchExecutor, InfluxdbExecutor, PostgresqlExecutor, RedisExecutor}
import querying.main.{Constants, LatencyLogger, QueryingUtils}
import querying.message._

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

object Federator {
  val extractEntityId: ShardRegion.ExtractEntityId = {
    case psq@PolyStoreQuery(_, _) => (psq.hashCode().toString, psq)
  }

  private val numberOfShards = 20

  val extractShardId: ShardRegion.ExtractShardId = {
    case psq@PolyStoreQuery(_, _) => (psq.hashCode % numberOfShards).toString
  }
}

class Federator extends Actor with ActorLogging {

  private var resultCount = 0
  private var results: Vector[Result] = Vector.empty
  private var querySender: Option[ActorRef] = None
  private var resultMap: HashMap[Int, Result] = HashMap.empty
  private var polyStoreQuery: Option[PolyStoreQuery] = None
  private var startTimeInMillis = 0L

  // === Deney-4: Federator timing (nanos) ===
  private var queryStartNanos: Long = 0L
  private var dispatchEndNanos: Long = 0L

  override def preStart(): Unit = {
    super.preStart
  }

  override def postStop(): Unit = {
    super.postStop
  }

  override def receive: Receive = {
    case psq@PolyStoreQuery(queryStoreMap, senderPath) =>
      // === Deney-4: total süresinin başlangıcı ===
      queryStartNanos = System.nanoTime()
      startTimeInMillis = System.currentTimeMillis()
      polyStoreQuery = Some(psq)
      val sizeInBytes = SizeEstimator.estimate(polyStoreQuery)
      log.info("Size of the FederateQuery message sent start Agent end Federator is: [{}] Bytes, and is [{}]", sizeInBytes, QueryingUtils.formatByteValue(sizeInBytes))
      log.debug("Hash Code for Federate Query: [{}], and Query Value: [{}]", psq.hashCode, psq)
      querySender = Some(sender())
      distribute(psq)
      // === Deney-4: dispatch fazı bitti ===
      dispatchEndNanos = System.nanoTime()
    case receivedResult@Result(_, _, _) =>
      processResult(receivedResult)
  }

  protected def processResult(receivedResult: Result): Unit = {
    if (receivedResult.key != 1) {
      resultMap += (receivedResult.key -> receivedResult)
    }
    val matched = seekForMatch(receivedResult)
    if (!matched)
      results = results :+ receivedResult

    if (resultCount == 0 && results.size == 1) {
      val sizeInBytes = SizeEstimator.estimate(receivedResult)
      log.info("Result has been constructed for the polystore query [{}]", polyStoreQuery.get)
      querySender.get ! receivedResult
      log.info("Federated query has been performed in: [{}] milliseconds", System.currentTimeMillis() - startTimeInMillis)
      log.info("Size of the result message sent start Federator end Sender is: [{}] Bytes, and is [{}]", sizeInBytes, QueryingUtils.formatByteValue(sizeInBytes))

      // === Deney-4: total süresi ölçümü + CSV log ===
      val totalNanos = System.nanoTime() - queryStartNanos
      val dispatchNanos = dispatchEndNanos - queryStartNanos
      val qid = polyStoreQuery.map(_.queryID).getOrElse("unknown")
      LatencyLogger.logFederationPhase(
        queryId = qid,
        totalMs = totalNanos / 1e6,
        dispatchMs = dispatchNanos / 1e6
      )

      self ! PoisonPill
    }
  }

  private def seekForMatch(receivedResult: Result): Boolean = {
    for {
      result <- results
      if JoinUtils.matchAnyVar(receivedResult.resultVars.asJava, result.resultVars.asJava)
    } {
      results = results.filterNot(res => res == result)
      // === Deney-5: queryId Federator → ParallelJoinManager iletimi (mevcut) ===
      val queryId: String = polyStoreQuery.map(_.queryID).getOrElse("unknown")
      val bucketDistributor = context.actorOf(ParallelJoinManager.props(queryId))
      bucketDistributor ! DistributeBuckets(receivedResult, result)
      resultCount -= 1
      return true
    }
    false
  }

  protected def distribute(polyStoreQuery: PolyStoreQuery): Unit = {
    resultCount = polyStoreQuery.queryStoreMap.size - 1
    val qid = polyStoreQuery.queryID // === Deney-4: queryId Executor'lara iletilecek ===

    for ((query, store) <- polyStoreQuery.queryStoreMap) {
      val executor: ActorRef = {
        store match {
          case Constants.REDIS => context.actorOf(RedisExecutor.props)
          case Constants.POSTGRESQL => context.actorOf(PostgresqlExecutor.props)
          case Constants.INFLUXDB => context.actorOf(InfluxdbExecutor.props)
          case Constants.ELASTICSEARCH => context.actorOf(ElasticsearchExecutor.props)
          case _ => ActorRef.noSender
        }
      }
      if (query.nonEmpty) {
        // === Deney-4: queryId parametresi ExecuteQuery mesajına ekli ===
        val executeQuery = ExecuteQuery(query, qid)
        executor ! executeQuery
        val sizeInBytes = SizeEstimator.estimate(executeQuery)
        log.info("Size of the ExecuteQuery message sent start Federator end [{}]Executor is: [{}] Bytes, and is [{}]", store, sizeInBytes, QueryingUtils.formatByteValue(sizeInBytes))
      }
    }
  }

}