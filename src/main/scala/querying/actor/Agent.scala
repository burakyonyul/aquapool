package querying.actor

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.client.ClusterClient
import querying.message.{PolyStoreQuery, Result}

object Agent {
  // Orijinal props — geriye dönük uyumluluk için
  // Bu versiyonda Agent kendi ClusterClient'ını oluşturur (eski davranış)
  def props: Props = Props(new Agent(None))

  // Yeni props — paylaşımlı ClusterClient ile
  // Tüm Agent'lar aynı ClusterClient'ı kullanır, TCP bağlantı çoğalması önlenir
  def props(sharedClient: ActorRef): Props = Props(new Agent(Some(sharedClient)))
}

class Agent(sharedClusterClient: Option[ActorRef]) extends Actor with ActorLogging {
  var start = 0L
  var senderActor: Option[ActorRef] = None

  override def receive: Receive = {
    case psq@PolyStoreQuery(_, queryID) =>
      start = System.nanoTime()

      // Paylaşımlı client varsa onu kullan, yoksa yeni oluştur (eski davranış)
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