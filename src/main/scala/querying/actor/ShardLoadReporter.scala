package querying.actor

import akka.actor.{Actor, ActorLogging, Cancellable, Props}
import akka.cluster.Cluster
import akka.cluster.sharding.ClusterSharding
import akka.cluster.sharding.ShardRegion.{ClusterShardingStats, GetClusterShardingStats}
import akka.pattern.ask
import akka.util.Timeout

import java.io.{File, FileWriter, PrintWriter}
import java.time.Instant
import scala.concurrent.duration._
import scala.util.Try

/**
 * ====================================================================
 * ShardLoadReporter — Load Distribution Proof Collector
 * ======================================================================= *
 * PURPOSE:
 * It runs on the AQuAPooL process (cluster member) side. Every ~5
 * seconds, it retrieves GetClusterShardingStats
 * from the cluster sharding API and writes the result to a CSV. This statistic shows how many
 * shards are active in each node and how many entities are alive in each shard. *
 * EVIDENCE VALUE:
 * In a 2-node configuration, the CSV lines should show:
 *
 * timestamp, node, shard_count, entity_count
 * 167...000, 155.223.25.1:2551, 50, ~100
 * 167...000, 155.223.25.2:2551, 50, ~100
 *
 * In a 1-node configuration:
 *
 * 167...000, 155.223.25.1:2551, 100, ~200
 *
 * The 2-node lines DIRECTLY prove approximately symmetric load distribution
 * — clearly cuts through reviewer 1's objection. *
 * STARTING:
 * In AQuAPooL's Main.scala, after the Federation region is started with ClusterSharding.start:
 *
 * val federatorRegion = ClusterSharding(system).start(
 * typeName = "Federator",
 * entityProps = Federationr.props(...),
 * settings = ClusterShardingSettings(system),
 * extractEntityId = ...,
 * extractShardId = ...
 * )
 *
 * system.actorOf(
 * ShardLoadReporter.props("Federator"),
 * "shard-load-reporter"
 * )
 *
 * OUTPUT:
 * shard_load_<host>_<port>.csv
 * Each node writes its own file (like MetricsListener). Collection
 * After the AQuAPooL process, it is retrieved to the experimental machine with scp. * ===================================================================
 */
object ShardLoadReporter {
  case object Tick

  def props(shardTypeName: String, intervalSeconds: Int = 5): Props =
    Props(new ShardLoadReporter(shardTypeName, intervalSeconds))
}

class ShardLoadReporter(shardTypeName: String, intervalSeconds: Int)
    extends Actor with ActorLogging {

  import ShardLoadReporter._
  import context.dispatcher

  private val cluster = Cluster(context.system)
  private val selfAddress = cluster.selfAddress
  private val shardRegion = ClusterSharding(context.system).shardRegion(shardTypeName)

  private implicit val askTimeout: Timeout = Timeout(3.seconds)

  // Each node has a separate file — no conflicts.
  private val host = Option(selfAddress.host).getOrElse("localhost")
  private val port = selfAddress.port.getOrElse(0)
  private val csvPath = s"shard_load_${host}_$port.csv"
  private val csvFile = new File(csvPath)
  private val isNew = !csvFile.exists()
  private val writer = new PrintWriter(new FileWriter(csvFile, true))
  if (isNew) {
    writer.println("timestamp_ms,reporter_node,observed_node,shard_count,entity_count")
    writer.flush()
  }

  private var schedule: Option[Cancellable] = None

  override def preStart(): Unit = {
    log.info(s"ShardLoadReporter started for type '$shardTypeName' on $selfAddress, " +
             s"writing to $csvPath every ${intervalSeconds}s")
    schedule = Some(context.system.scheduler.scheduleAtFixedRate(
      initialDelay = 5.seconds,
      interval = intervalSeconds.seconds,
      receiver = self,
      message = Tick
    ))
  }

  override def postStop(): Unit = {
    schedule.foreach(_.cancel())
    Try(writer.close())
    log.info(s"ShardLoadReporter stopped, $csvPath finalized.")
  }

  def receive: Receive = {
    case Tick =>
      // Request the shard distribution statistic across the cluster.
      // It's enough for just one node to request it (cluster-wide), but it's okay if every
      // node requests it — it will just add more rows.
      // Here, only OUR OWN node should request it: the node with the smallest address
      // takes responsibility.
      val members = cluster.state.members
        .filter(m => m.status == akka.cluster.MemberStatus.Up)
        .map(_.address)
        .toSeq
        .sorted(Ordering.by((a: akka.actor.Address) =>
          (a.host.getOrElse(""), a.port.getOrElse(0))))

      val isLeader = members.headOption.contains(selfAddress)
      if (isLeader) {
        (shardRegion ? GetClusterShardingStats(askTimeout.duration))
          .recover { case _ => ClusterShardingStats(Map.empty) }
          .foreach {
            case ClusterShardingStats(regions) =>
              val ts = Instant.now().toEpochMilli
              if (regions.isEmpty) {
                writer.println(s"$ts,$selfAddress,(empty),0,0")
              } else {
                regions.foreach { case (addr, shardStats) =>
                  val shardCount = shardStats.stats.size
                  val entityCount = shardStats.stats.values.sum
                  writer.println(
                    s"$ts,${selfAddress.host.getOrElse("?")}:${selfAddress.port.getOrElse(0)}," +
                      s"${addr.host.getOrElse("?")}:${addr.port.getOrElse(0)}," +
                      s"$shardCount,$entityCount"
                  )
                }
              }
              writer.flush()
            case _ => // unexpected response
          }
      }
  }
}
