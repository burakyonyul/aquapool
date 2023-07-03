package querying.actor

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.client.{ClusterClient, ClusterClientSettings}
import com.hp.hpl.jena.query.ResultSetFormatter
import querying.message.{PolyStoreQuery, Result}

object Agent {
  def props: Props = Props(new Agent)
}

class Agent extends Actor with ActorLogging {
  var start = 0L

  override def receive: Receive = {
    case psq@PolyStoreQuery(_, _) =>
      start = System.nanoTime()
      val clusterClient = context.system.actorOf(ClusterClient.props(ClusterClientSettings(context.system)), "client")
      clusterClient ! ClusterClient.Send("/system/sharding/Federator", PolyStoreQuery(psq.queryStoreMap, self.path.toString), localAffinity = true)
      log.info("Polystore query has been sent to AXE")

    case result@Result(_, _, _) =>
      //log.info("Result has been received. Current query count: [{}], and current actor count: [{}]", MetricStore.get(Constants.QUERY_COUNT).get, MetricStore.get(Constants.ACTOR_COUNT).get)
      val duration = (System.nanoTime() - start) / 1e9d
      log.info("Result has been received. [{}] in [{}] seconds", result, duration)
      val resultSet = result.toResultSet
      ResultSetFormatter.out(resultSet)
    case message@_ =>
      log.info("Received unknown message: [{}]", message)
  }
}
