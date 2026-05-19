package querying

import akka.actor.{ActorSystem, Props}
import akka.cluster.client.ClusterClientReceptionist
import akka.cluster.metrics.ClusterMetricsExtension
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings}
import com.typesafe.config.ConfigFactory
import querying.actor._
import querying.actor.federation.Federator

import java.net._

object App {

  val CLEAN = "clean"

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      startup(Seq("127.0.0.1", "2551"))
    }
    else {
      startup(args)
    }
  }

  private def getIpAddress: String = {
    val e = NetworkInterface.getNetworkInterfaces
    if (e.hasMoreElements) {
      val n = e.nextElement match {
        case e: NetworkInterface => e
        case _ => ???
      }
      val ee = n.getInetAddresses
      if (ee.hasMoreElements) {
        ee.nextElement match {
          case e: InetAddress => return e.getHostAddress
          case _ => ???
        }
      }
    }
    "127.0.0.1"
  }


  def startup(args: Seq[String]): Unit = {
    //println(System.getenv("aeron.term.buffer.length"))
    // In a production application you wouldn't typically start multiple ActorSystem instances in the
    // same JVM, here we do fluxRecord end easily demonstrate these ActorSytems (which would be in separate JVM's)
    // talking end each other.
    var ipAddress = getIpAddress
    var port = "2551"
    if (args.size > 1) {
      ipAddress = args.head
      port = args(1)
    }
    if (args.size == 3 && args(2) == CLEAN) {
      //MetricStoreUtils.deleteStore
    }
    // Override the configuration of the port
    val config = ConfigFactory.parseString("akka.remote.artery.canonical.hostname = " + ipAddress).
      withFallback(ConfigFactory.parseString("akka.remote.artery.canonical.port = " + port)).
      withFallback(ConfigFactory.load())

    println(s"Dispatcher parallelism: " +
      ConfigFactory.load().getInt(
        "akka.actor.default-dispatcher.fork-join-executor.parallelism-min"
      ))
    println(s"Bucket size: " +
      ConfigFactory.load().getInt("aquapool.join.bucket-size"))
    println(s"Log file: " +
      ConfigFactory.load().getString("aquapool.join.log-file"))

    // Create an Akka system
    val system = ActorSystem("Querying", config)
    // Create an actor that starts the sharding and sends random messages

    ClusterMetricsExtension(system).subscribe(system.actorOf(MetricsListener.props))

    val federatorRegion = ClusterSharding(system).start(
      typeName = "Federator",
      entityProps = Props[Federator],
      settings = ClusterShardingSettings(system),
      extractEntityId = Federator.extractEntityId,
      extractShardId = Federator.extractShardId)

    ClusterClientReceptionist(system).registerService(federatorRegion)
    system.log.info("Polystore system has been started.")
    //println("Polystore system has been started.")

    // After a cluster join — where other actors are launched
    system.actorOf(querying.actor.MetricsListener.props, "metrics-listener")
    system.actorOf(
      ShardLoadReporter.props("Federator"),  // Federator = sizin shard type name
      "shard-load-reporter"
    )

    /*
    ClusterSharding(system).start(
      typeName = "Distributor",
      entityProps = Props[Distributor],
      settings = ClusterShardingSettings(system),
      extractEntityId = Distributor.extractEntityId,
      extractShardId = Distributor.extractShardId)
     */

    /*
    ClusterSharding(system).start(
      typeName = "RdfStoreExecutor",
      entityProps = Props[RdfStoreExecutor],
      settings = ClusterShardingSettings(system),
      extractEntityId = RdfStoreExecutor.extractEntityId,
      extractShardId = RdfStoreExecutor.extractShardId)
     */
    /*
    ClusterSharding(system).start(
      typeName = "ParallelJoinManager",
      entityProps = Props[ParallelJoinManager],
      settings = ClusterShardingSettings(system),
      extractEntityId = ParallelJoinManager.extractEntityId,
      extractShardId = ParallelJoinManager.extractShardId)

    ClusterSharding(system).start(
      typeName = "HashJoinPerformer",
      entityProps = Props[HashJoinPerformer],
      settings = ClusterShardingSettings(system),
      extractEntityId = HashJoinPerformer.extractEntityId,
      extractShardId = HashJoinPerformer.extractShardId)
    */
  }
}