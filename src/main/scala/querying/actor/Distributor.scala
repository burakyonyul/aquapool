package querying.actor

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.cluster.sharding.{ClusterSharding, ShardRegion}
import com.hp.hpl.jena.query.{ResultSetFactory, ResultSetFormatter}
import com.hp.hpl.jena.sparql.engine.binding.Binding
import main.QueryIterCollection
import org.apache.spark.util.SizeEstimator
import play.api.libs.json.Json
import querying.main.MonitoringUtils
import querying.message.{DistributeServiceClause, ExecuteServiceClause, Result}

import java.io.ByteArrayOutputStream
import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap


object Distributor {
  val extractEntityId: ShardRegion.ExtractEntityId = {
    case msg@DistributeServiceClause(query, _) => (query.hashCode.toString, msg)
  }

  private val numberOfShards = 20

  val extractShardId: ShardRegion.ExtractShardId = {
    case DistributeServiceClause(query, _) => (query.hashCode % numberOfShards).toString
  }
}

class Distributor extends Actor with ActorLogging {

  private var federator: Option[ActorRef] = None
  private var resultCount = 0
  private var resultMap: HashMap[Int, Result] = HashMap.empty
  private var distributeServiceClause: Option[DistributeServiceClause] = None

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
    case dsc@DistributeServiceClause(query, endpoints) =>
      //log.info("Sender path: {}, self path {}",sender().path,self.path)
      distributeServiceClause = Some(dsc)
      log.debug("Hash Code for Distribute SERVICE Clause: [{}], and Query Value: [{}], Endpoint Values: [{}]", dsc.hashCode, query, endpoints)
      distribute(query, endpoints)
      federator = Some(sender())
      resultCount = endpoints.size
    case result@Result(_, _, key) =>
      resultCount -= 1
      resultMap += (key -> result)
      if (resultCount == 0) {
        val finalRes = constructResult
        federator.get ! finalRes
        val sizeInBytes = SizeEstimator.estimate(finalRes)
        log.info("Size of the new result message sent from Distributor to Federator is: [{}] Bytes, and is [{}].", sizeInBytes, MonitoringUtils.formatByteValue(sizeInBytes))
      }
  }

  private def constructResult = {
    val finalResultSet = ResultSetFactory.create(new QueryIterCollection(generateBindings.asJava), resultMap.values.head.resultVars.asJava)
    val outputStream = new ByteArrayOutputStream
    ResultSetFormatter.outputAsJSON(outputStream, finalResultSet)
    val finalResult = Result(Json.parse(outputStream.toByteArray), finalResultSet.getResultVars.asScala, distributeServiceClause.get.hashCode)
    finalResult
  }

  private def generateBindings = {
    var bindingList: Vector[Binding] = Vector.empty
    for ((_, v) <- resultMap) {
      val rs = v.toResultSet
      while (rs.hasNext)
        bindingList = bindingList :+ rs.nextBinding
    }
    bindingList
  }

  protected def distribute(query: String, endpoints: Seq[String]) = {
    endpoints foreach {
      endpoint =>
        val executorRegion = ClusterSharding.get(context.system).shardRegion("Executor")
        val executeServiceClause = ExecuteServiceClause(query, endpoint)
        executorRegion ! executeServiceClause
        val sizeInBytes = SizeEstimator.estimate(executeServiceClause)
        log.info("Size of the ExecuteServiceClause message sent from Distributor to Executor is: [{}] Bytes, and is [{}]", sizeInBytes, MonitoringUtils.formatByteValue(sizeInBytes))
    }

  }

}
