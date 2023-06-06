package querying.actor

import akka.actor.{Actor, ActorLogging, Props}
import org.apache.spark.util.SizeEstimator
import querying.main.MonitoringUtils
import querying.message.ExecuteQuery
import querying.message.Store._

object Executor {
  /*
  val extractEntityId: ShardRegion.ExtractEntityId = {
    case esc@ExecuteQuery(_, _) => (esc.hashCode.toString, esc)
  }

  private val numberOfShards = 20

  val extractShardId: ShardRegion.ExtractShardId = {
    case esc@ExecuteQuery(_, _) => (esc.hashCode % numberOfShards).toString
  }
   */
  def props: Props = Props(new Executor)
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
    case esc@ExecuteQuery(query, store) =>
      //log.info("Sender path: {}, self path {}",sender().path,self.path)
      log.debug("Hash Code for Execute SERVICE Clause: [{}], and Query Value: [{}], Endpoint Value: [{}]", esc.hashCode, query, store)
      val result = executeQuery(query, store)
      sender ! result
      val sizeInBytes = SizeEstimator.estimate(result)
      log.info("Size of the new result message sent from Executor to Distributor is: [{}] Bytes, and is [{}]", sizeInBytes, MonitoringUtils.formatByteValue(sizeInBytes))
  }

  protected def executeQuery(query: String, store: Store) = {
    /*
    val execution = QueryExecutionFactory.sparqlService(store, query)
    val result = MonitoringUtils.convertRdf2Result(execution.execSelect())
    execution.close()
    Result(result.resultJSON, result.resultVars, store.hashCode)
     */
    store match {
      case Redis => println("Redis query");
      case Postgresql => println("Postgresql query")
      case Influxdb => println("InfluxDB query")
      case Elasticsearch => println("Elasticsearch query")
      case _ => println("Unknown store query")
    }
    None
  }

}