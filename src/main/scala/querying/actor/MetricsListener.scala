package querying.actor

import akka.actor.{Actor, ActorLogging, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.metrics.{ClusterMetricsChanged, ClusterMetricsExtension, NodeMetrics}
import akka.cluster.metrics.StandardMetrics.{Cpu, HeapMemory}

import java.io.{File, FileWriter, PrintWriter}
import java.time.Instant
import scala.util.Try

/**
 * =====================================================================
 * MetricsListener — Cluster-wide CPU/Heap toplayıcı
 * =====================================================================
 *
 * AMAÇ:
 *   sigar-loader üzerinden gelen ClusterMetricsChanged event'lerini
 *   her node için ayrı bir CSV dosyasına yazar. Throughput deneyi
 *   sonrasında bu CSV'ler analiz edilerek "ikinci node gerçekten
 *   yük aldı mı?" sorusu cevaplanır.
 *
 * NEDEN DEBUG LOG DEĞİL CSV?
 *   Eski sürüm log.debug ile bilgi basıyordu, ama (a) DEBUG seviyesi
 *   kapatılınca kayboluyor, (b) log dosyasından parse etmek zor,
 *   (c) makaledeki tabloya doğrudan koyamıyoruz. CSV ise pandas/
 *   numpy ile saniyeler içinde özetlenir.
 *
 * NE TOPLANIYOR?
 *   - Timestamp (epoch ms)
 *   - Node address (hangi sunucu)
 *   - CPU system load average (1 min)
 *   - CPU combined utilization (0.0-1.0, sigar varsa)
 *   - Heap used (MB)
 *   - Heap committed (MB)
 *   - Heap max (MB)
 *   - Processor count (her node'da 16 olmalı, doğrulama için)
 *
 * NEREYE YAZILIYOR?
 *   ./metrics_<hostname>_<port>.csv
 *   Her node kendi dosyasına yazar, böylece çakışma olmaz.
 *
 * KONFIGÜRASYON:
 *   application.conf'ta zaten ClusterMetricsExtension yüklü.
 *   Ek bir ayar gerekmez. Bu actor cluster ayağa kalktıktan sonra
 *   herhangi bir noktada başlatılabilir (örn. Main.scala'da).
 *
 * BAŞLATMA (Main.scala'ya eklenmeli):
 *   system.actorOf(MetricsListener.props, "metrics-listener")
 * =====================================================================
 */
object MetricsListener {
  def props: Props = Props(new MetricsListener)
}

class MetricsListener extends Actor with ActorLogging {
  private val selfAddress = Cluster(context.system).selfAddress
  private val extension = ClusterMetricsExtension(context.system)

  // ─── CSV dosyası: her node ayrı dosya yazar ────────────────────────
  private val host = Option(selfAddress.host).getOrElse("localhost")
  private val port = selfAddress.port.getOrElse(0)
  private val csvPath = s"metrics_${host}_$port.csv"
  private val csvFile = new File(csvPath)
  private val isNew = !csvFile.exists()

  private val writer = new PrintWriter(new FileWriter(csvFile, true)) // append modu
  if (isNew) {
    writer.println("timestamp_ms,node,cpu_load_avg,cpu_combined,heap_used_mb,heap_committed_mb,heap_max_mb,processors")
    writer.flush()
  }

  override def preStart(): Unit = {
    extension.subscribe(self)
    log.info(s"MetricsListener subscribed for $selfAddress, writing to $csvPath")
  }

  override def postStop(): Unit = {
    extension.unsubscribe(self)
    Try(writer.close())
    log.info(s"MetricsListener stopped, $csvPath finalized.")
  }

  def receive: Receive = {
    case ClusterMetricsChanged(clusterMetrics) =>
      // Sadece kendi node'umuzun metric'lerini yazıyoruz; diğer node
      // zaten kendi dosyasına yazıyor olacak.
      clusterMetrics.filter(_.address == selfAddress).foreach(writeRow)

    case _: CurrentClusterState =>
    // Cluster up event — yoksay
  }

  private def writeRow(nm: NodeMetrics): Unit = {
    val ts = Instant.now().toEpochMilli

    var cpuLoad: Double = -1.0
    var cpuComb: Double = -1.0
    var procs: Int = -1
    nm match {
      case Cpu(_, _, Some(load), combined, _, processors) =>
        cpuLoad = load
        cpuComb = combined.getOrElse(-1.0)
        procs = processors
      case _ => // CPU bilgisi yoksa varsayılan kalır
    }

    var used: Double = -1.0
    var committed: Double = -1.0
    var max: Double = -1.0
    nm match {
      case HeapMemory(_, _, u, c, m) =>
        used = u.doubleValue / (1024 * 1024)
        committed = c.doubleValue / (1024 * 1024)
        max = m.map(_.doubleValue / (1024 * 1024)).getOrElse(-1.0)
      case _ => // Heap bilgisi yoksa varsayılan kalır
    }

    writer.println(
      f"$ts,${selfAddress.host.getOrElse("?")}:${selfAddress.port.getOrElse(0)}," +
        f"$cpuLoad%.4f,$cpuComb%.4f,$used%.2f,$committed%.2f,$max%.2f,$procs"
    )
    writer.flush()
  }
}