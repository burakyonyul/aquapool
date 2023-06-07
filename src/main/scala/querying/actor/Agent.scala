package querying.actor

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.client.{ClusterClient, ClusterClientSettings}
import querying.message.{PolyStoreQuery, Result}

object Agent {
  def props: Props = Props(new Agent)
}

class Agent extends Actor with ActorLogging {
  override def receive: Receive = {
    case psq@PolyStoreQuery(_, _) =>
      val clusterClient = context.system.actorOf(ClusterClient.props(ClusterClientSettings(context.system)), "client")
      clusterClient ! ClusterClient.Send("/system/sharding/Federator", psq, localAffinity = true)
      log.info("Poly Store query has been sent to AXE")

    case result@Result(_, _, _) =>
      //log.info("Result has been received. Current query count: [{}], and current actor count: [{}]", MetricStore.get(Constants.QUERY_COUNT).get, MetricStore.get(Constants.ACTOR_COUNT).get)
      log.info("Result has been received. [{}]", result)
    case message@_ =>
      log.info("Received unknown message: [{}]", message)
  }
}
