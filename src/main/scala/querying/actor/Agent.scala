package querying.actor

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.client.{ClusterClient, ClusterClientSettings}
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
      val resultSet = result.toResultSet
      var rows = 0
      while (resultSet.hasNext) {
        resultSet.next()
        rows += 1
      }
      log.info("Result has been received. in [{}] seconds and has [{}] rows", duration, rows)
    //ResultSetFormatter.out(resultSet)
    case message@_ =>
      log.info("Received unknown message: [{}]", message)
  }
}
