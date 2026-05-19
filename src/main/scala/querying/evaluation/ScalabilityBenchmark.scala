package querying.evaluation

import akka.actor.{ActorSystem, PoisonPill}
import akka.cluster.client.{ClusterClient, ClusterClientSettings}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import querying.actor.Agent
import querying.message.PolyStoreQuery

import java.io.{FileWriter, PrintWriter}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * =====================================================================
 * Scalability (Cluster Sharding) Throughput Benchmark
 * =====================================================================
 *
 * AMAÇ:
 *   Akka cluster sharding'in throughput'a katkısını kanıtlamak.
 *   1-node AQuAPooL vs 2-node AQuAPooL karşılaştırması.
 *
 * TASARIM:
 *   - TEK PAYLAŞIMLI ClusterClient: Tüm Agent'lar aynı TCP bağlantısını
 *     kullanır. Bu sayede 50-100 eşzamanlı sorguda receptionist ezilmez.
 *   - HER SORGU İÇİN AYRI AGENT: Mailbox FIFO sorununu ve senderActor
 *     state ezilmesini önler. Ama hepsi aynı ClusterClient'ı kullanır.
 *   - SENDERPATH VARYASYONU: Farklı hashCode → farklı shard → farklı
 *     Federator → node'lara dağılım.
 *
 *   Akış:
 *     Benchmark → Agent-1 ─┐
 *     Benchmark → Agent-2 ──┤── Paylaşımlı ClusterClient ── Cluster
 *     Benchmark → Agent-3 ──┤       (tek TCP bağlantısı)
 *     ...                   │
 *     Benchmark → Agent-N ─┘
 *
 * ÖN KOŞUL:
 *   Agent.scala'da sharedClusterClient desteği eklenmiş olmalı:
 *     object Agent {
 *       def props(sharedClient: ActorRef): Props = Props(new Agent(Some(sharedClient)))
 *     }
 *
 *   Federator.scala'da hashCode tüm mesajı kapsamalı:
 *     case msg@PolyStoreQuery(_, _) => (msg.hashCode.toString, msg)
 *
 * ÇALIŞTIRMA:
 *   sbt "runMain querying.ScalabilityBenchmark 1node"
 *   sbt "runMain querying.ScalabilityBenchmark 2node"
 * =====================================================================
 */
object ScalabilityBenchmark {

  implicit val timeout: Timeout = Timeout(10.minutes)
  val THROUGHPUT_REPEATS = 5

  // ─── Query pool (Same queries as AquaPoolBenchmark, strings untouched)
  val queryPool: Seq[(String, Map[String, String])] = Seq(
    ("A.2.11", Queries.Query_A_2_11),  // InfluxDB temporal
    ("A.2.12", Queries.Query_A_2_12),  // InfluxDB temporal, ~0.1s (light)
    ("A.4.3",  Queries.Query_A_4_3),   // Redis, ~0.01s
    ("A.4.7",  Queries.Query_A_4_7),   // Redis, ~0.01s
    ("A.4.9",  Queries.Query_A_4_9),   // Redis, ~0.01s
    ("A.6.4",  Queries.Query_A_6_4),   // ES, 3 rows, ~0.7s (light ES)
    ("A.6.5",  Queries.Query_A_6_5),   // ES, 1 row, ~0.5s (lightest ES)
    ("A.8.1",  Queries.Query_A_8_1),   // R+I compound, ~1.6s (mid)
    ("A.8.5",  Queries.Query_A_8_5),   // R+P compound, ~0.03s (light)
    ("A.8.7",  Queries.Query_A_8_7)   // R+P+E+I compound
  )

  def main(args: Array[String]): Unit = {

    val nodeConfig = if (args.isDefinedAt(0)) args(0) else "2node"
    val ipAddress  = if (args.isDefinedAt(1)) args(1) else "127.0.0.1"
    val port       = if (args.isDefinedAt(2)) args(2) else "2553"

    println("=" * 70)
    println("  Scalability Throughput Benchmark")
    println(s"  Configuration: $nodeConfig AQuAPooL")
    println("=" * 70)

    // ─── Start the actor system
    val config = ConfigFactory.parseString(s"akka.remote.artery.canonical.hostname = $ipAddress")
      .withFallback(ConfigFactory.parseString(s"akka.remote.artery.canonical.port = $port"))
      .withFallback(ConfigFactory.load("agent.conf"))

    val system = ActorSystem("ScalabilityBenchmark", config)
    implicit val ec: ExecutionContext = system.dispatcher

    // ─────────────────────────────────────────────────────────────────
    // CREATE A SINGLE SHARED ClusterClient
    // All Agents will use this client.
    // Single TCP connection → receptionist is not overwhelmed.
    // The ClusterClient performs multiplexing internally — it can send multiple
    // messages simultaneously over the same connection.
    // ─────────────────────────────────────────────────────────────────
    val sharedClusterClient = system.actorOf(
      ClusterClient.props(ClusterClientSettings(system)),
      "shared-cluster-client"
    )

    // Short wait for ClusterClient to connect.
    Thread.sleep(2000)
    println("  A shared cluster client has been created.\n")

    // ─── Warm-up
    println("[Warm-up] The system is being heated with 3 queries....")
    for (i <- 1 to 3) {
      val (qName, qMap) = queryPool(i % queryPool.size)
      val warmupAgent = system.actorOf(Agent.props(sharedClusterClient), s"WarmupAgent-$i")
      val result = Try(Await.result(
        warmupAgent ? PolyStoreQuery(qMap, s"warmup-$i"),
        timeout.duration
      ))
      warmupAgent ! PoisonPill
      println(s"  Warm-up $i/3 ($qName): ${if (result.isSuccess) "OK" else "HATA"}")
    }
    Thread.sleep(1000)
    println("  System is ready.\n")

    // ─── Shard distribution control
    println("  [Shard distribution control]")
    val shardSet = scala.collection.mutable.Set[Int]()
    for (i <- 0 until 10) {
      val (qName, qMap) = queryPool(i % queryPool.size)
      val psq = PolyStoreQuery(qMap, s"variation-$i")
      val shardId = math.abs(psq.hashCode % 20)
      shardSet += shardId
      println(f"    $qName%-10s senderPath=variation-$i → shard=$shardId%3d")
    }
    if (shardSet.size <= 1) {
      println("\n  ⚠ WARNING: All queries land in the SAME shard.!")
      println("  Check the changes to federator.scala..\n")
    } else {
      println(s"  ✓There is a distribution across ${shardSet.size} different shards.\n")
    }

    // ─── Throughput test
    val outputFile = s"throughput_results_$nodeConfig.csv"
    val csvWriter = new PrintWriter(new FileWriter(outputFile))
    csvWriter.println("config,concurrency,repeat,total_queries,completed,failed," +
      "total_time_ms,throughput_qps,mean_latency_ms,std_latency_ms,min_latency_ms,max_latency_ms")

    val concurrencyLevels = Seq(10, 20, 30)

    println("[Throughput Testi]")
    println(s"  Configuration:    $nodeConfig AQuAPooL")
    println(s"  ClusterClient:    paylaşımlı (tek TCP bağlantısı)")
    println(s"  Query pool:     ${queryPool.size} sorgu × senderPath varyasyonu")
    println(s"  Concurrency:      ${concurrencyLevels.mkString(", ")}")
    println(s"  Repeat:           $THROUGHPUT_REPEATS")
    println(s"  Output:            $outputFile")
    println()

    for (concurrency <- concurrencyLevels) {
      println(s"  ┌─── Concurrency: $concurrency concurrent queries ───┐")

      for (repeat <- 1 to THROUGHPUT_REPEATS) {

        // ───────────────────────────────────────────────────────────
        // Create a separate Agent for each query (senderActor state isolation)
        // BUT they all use the same sharedClusterClient (single TCP)
        // ───────────────────────────────────────────────────────────
        val batchStartTime = System.nanoTime()

        val futures: Seq[Future[(String, Long, Boolean)]] = (0 until concurrency).map { i =>
          val (qName, qMap) = queryPool(i % queryPool.size)
          val uniqueSenderPath = s"C$concurrency-R$repeat-Q$i"
          val agentName = s"Agent-$uniqueSenderPath"

          // The agent is created with a shared ClusterClient.
          val agent = system.actorOf(Agent.props(sharedClusterClient), agentName)
          val queryStartTime = System.nanoTime()

          (agent ? PolyStoreQuery(qMap, uniqueSenderPath)).map { _ =>
            val latency = System.nanoTime() - queryStartTime
            agent ! PoisonPill
            (qName, latency, true)
          }.recover { case ex: Exception =>
            val latency = System.nanoTime() - queryStartTime
            agent ! PoisonPill
            println(s"    ⚠ $qName ($agentName): ${ex.getMessage}")
            (qName, latency, false)
          }
        }

        // ─── Wait for all the results.
        val latencies = new ArrayBuffer[Long]()
        var completed = 0
        var failed = 0

        for (f <- futures) {
          Try(Await.result(f, timeout.duration)) match {
            case Success((_, latency, true)) =>
              latencies += latency
              completed += 1
            case Success((_, _, false)) =>
              failed += 1
            case Failure(ex) =>
              failed += 1
              println(s"    ⚠ Await: ${ex.getMessage}")
          }
        }

        // ─── Statistics
        val batchElapsedMs = (System.nanoTime() - batchStartTime) / 1e6
        val throughput = if (batchElapsedMs > 0) completed / (batchElapsedMs / 1000.0) else 0.0

        val latMs = latencies.map(_ / 1e6)
        val meanLat = if (latMs.nonEmpty) latMs.sum / latMs.size else 0.0
        val stdLat = if (latMs.size > 1) {
          math.sqrt(latMs.map(l => math.pow(l - meanLat, 2)).sum / (latMs.size - 1))
        } else 0.0
        val minLat = if (latMs.nonEmpty) latMs.min else 0.0
        val maxLat = if (latMs.nonEmpty) latMs.max else 0.0

        csvWriter.println(
          f"$nodeConfig,$concurrency,$repeat,$concurrency,$completed,$failed," +
            f"$batchElapsedMs%.0f,$throughput%.4f,$meanLat%.2f,$stdLat%.2f,$minLat%.2f,$maxLat%.2f"
        )
        csvWriter.flush()

        println(f"  │ #$repeat: $completed%3d/$concurrency%3d OK" +
          f", $throughput%.2f q/s" +
          f", lat=${meanLat}%.0f±${stdLat}%.0f ms" +
          f" [${minLat}%.0f–${maxLat}%.0f ms]" +
          (if (failed > 0) s", ⚠ $failed FAILS" else ""))

        // Batch cleaning (for processing Agent Poison Pills)
        Thread.sleep(3000)
      }

      println(s"  └${"─" * 46}┘\n")
    }

    csvWriter.close()

    // ─── Close
    println("=" * 70)
    println(s"  $nodeConfig testi tamamlandı!")
    println(s"  Sonuçlar: $outputFile")
    println("=" * 70)

    // Also closte the ClusterClient
    sharedClusterClient ! PoisonPill
    Thread.sleep(1000)

    system.terminate()
    Await.result(system.whenTerminated, 30.seconds)
  }
}