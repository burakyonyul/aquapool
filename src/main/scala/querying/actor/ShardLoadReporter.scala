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
 * =====================================================================
 * ShardLoadReporter — Yük Dağılım Kanıtı Toplayıcı
 * =====================================================================
 *
 * AMAÇ:
 *   AQuAPooL süreci (cluster member) tarafında çalışır. Her ~5
 *   saniyede bir cluster sharding API'sinden GetClusterShardingStats
 *   alır ve sonucu CSV'ye yazar. Bu istatistik, her node'da kaç
 *   shard'ın aktif olduğunu ve her shard'da kaç entity'nin canlı
 *   olduğunu gösterir.
 *
 * KANIT DEĞERİ:
 *   2-node konfigürasyonunda CSV satırları şunu göstermeli:
 *
 *      timestamp, node, shard_count, entity_count
 *      167...000, 155.223.25.1:2551, 50, ~100
 *      167...000, 155.223.25.2:2551, 50, ~100
 *
 *   1-node konfigürasyonunda:
 *
 *      167...000, 155.223.25.1:2551, 100, ~200
 *
 *   2-node satırları yaklaşık simetrik yük dağılımını DOĞRUDAN
 *   kanıtlar — hakem 1'in itirazını net şekilde keser.
 *
 * BAŞLATMA:
 *   AQuAPooL'un Main.scala'sında, ClusterSharding.start ile Federator
 *   shard region başlatıldıktan SONRA:
 *
 *     val federatorRegion = ClusterSharding(system).start(
 *       typeName = "Federator",
 *       entityProps = Federator.props(...),
 *       settings = ClusterShardingSettings(system),
 *       extractEntityId = ...,
 *       extractShardId = ...
 *     )
 *
 *     system.actorOf(
 *       ShardLoadReporter.props("Federator"),
 *       "shard-load-reporter"
 *     )
 *
 * ÇIKTI:
 *   shard_load_<host>_<port>.csv
 *   Her node kendi dosyasını yazar (MetricsListener gibi). Toplama
 *   AQuAPooL süreci sonrası deney makinesine scp ile alınır.
 * =====================================================================
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

  // Her node ayrı dosya — çakışma yok
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
      // Cluster genelinde shard dağılım istatistiğini iste.
      // Sadece bir node'un istemesi yeter (cluster-wide), ama her
      // node istese de zarar yok — sadece daha çok satır olur.
      // Burada sadece KENDİ node'umuz istesin: en küçük adresli node
      // sorumluluğu üstlenir.
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
            case _ => // beklenmeyen yanıt
          }
      }
  }
}
