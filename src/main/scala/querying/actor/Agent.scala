package querying.actor

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.client.ClusterClient
import querying.message.{PolyStoreQuery, Result}

object Agent {
  // Original props — for backward compatibility.
  // In this version, the Agent creates its own ClusterClient (old behavior)
  def props: Props = Props(new Agent(None))

  // New props — with shared ClusterClient
  // All Agents use the same ClusterClient, TCP connection duplication is prevented
  def props(sharedClient: ActorRef): Props = Props(new Agent(Some(sharedClient)))
}

class Agent(sharedClusterClient: Option[ActorRef]) extends Actor with ActorLogging {
  var start = 0L
  var senderActor: Option[ActorRef] = None

  override def receive: Receive = {
    case psq@PolyStoreQuery(_, queryID) =>
      start = System.nanoTime()

      // If you have a shared client, use it; otherwise, create a new one (old behavior)
      val clusterClient = sharedClusterClient.getOrElse {
        import akka.cluster.client.ClusterClientSettings
        context.system.actorOf(
          ClusterClient.props(ClusterClientSettings(context.system)),
          s"client-${queryID + System.nanoTime()}"
        )
      }

      clusterClient ! ClusterClient.Send(
        "/system/sharding/Federator",
        PolyStoreQuery(psq.queryStoreMap, self.path.toString),
        localAffinity = true
      )
      log.info("Polystore query has been sent to AXE")
      senderActor = Some(sender())

    case result@Result(_, _, _) =>
      senderActor.foreach(_ ! result)

    case message@_ =>
      log.info("Received unknown message: [{}]", message)
  }
}