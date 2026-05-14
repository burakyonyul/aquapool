package querying.evaluation

import akka.actor.{ActorSystem, PoisonPill}
import akka.cluster.client.{ClusterClient, ClusterClientSettings}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import querying.actor.Agent
import querying.message.PolyStoreQuery

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success, Try}

/**
 * =====================================================================
 * Deney-4: Latency Breakdown Benchmark
 * =====================================================================
 *
 * AMAÇ:
 * Her compound sorgunun toplam yürütme süresini 4 faza ayırmak:
 *     1. Store Execution     — backend DB sorgusu (per-store)
 *        2. RDF Transformation  — polystore global model'e dönüşüm
 *        3. Parallel Hash Join  — ParallelJoinManager süresi
 *        4. Federation Overhead — dispatch + aggregation + messaging
 *
 * KONFİGÜRASYON (CLUSTER):
 *   - Thread pool = 16 (Deney 5'in optimum noktası)
 *   - Bucket size = 1000 (Deney 5'in optimum noktası)
 *
 * ÖLÇÜM (üç CSV birleştirilerek hesaplanır):
 *   - federation_latency.csv:  total_ms, dispatch_ms
 *   - executor_latency.csv:    store_exec_ms, transform_ms (per-store)
 *   - join_latency.csv:        join_time_ms (per-stage; Deney-5 logger)
 *
 * Federation Overhead = total - max(store_exec across stores)
 *                              - max(transform across stores)
 *                              - sum(join_time across stages)
 *
 * ÇALIŞTIRMA:
 * # Terminal-1 (cluster node):
 * sbt -J-Xmx8g \
 * -Daquapool.dispatcher.parallelism=16 \
 * -Daquapool.join.bucket-size=1000 \
 * -Daquapool.join.log-file=join_latency.csv \
 * -Daquapool.latency.executor-log=executor_latency.csv \
 * -Daquapool.latency.federation-log=federation_latency.csv \
 * "runMain querying.App 155.223.25.1 2551"
 *
 * # Terminal-2 (benchmark):
 * sbt "runMain querying.evaluation.LatencyBreakdownBenchmark"
 *
 * SORGULAR: 7 compound (A.8.1 - A.8.7)
 * RUN: 3 warm-up + 10 measurement = 13 run/sorgu = 91 toplam
 * SÜRE: ~1.5-2 saat
 */
object LatencyBreakdownBenchmark {

  implicit val timeout: Timeout = Timeout(20.minutes)

  val WARMUP_RUNS = 3
  val MEASURE_RUNS = 10

  val compoundQueries: Seq[(String, Map[String, String])] = Seq(
    // ─── Time-Series Single Queries (A.2.x → InfluxDB üzerinden) ───
    ("A.2.1", Queries.Query_A_2_1),
    ("A.2.2", Queries.Query_A_2_2),
    ("A.2.3", Queries.Query_A_2_3),
    ("A.2.4", Queries.Query_A_2_4),
    ("A.2.5", Queries.Query_A_2_5),
    ("A.2.6", Queries.Query_A_2_6),
    ("A.2.7", Queries.Query_A_2_7),
    ("A.2.8", Queries.Query_A_2_8),
    ("A.2.9", Queries.Query_A_2_9),
    ("A.2.10", Queries.Query_A_2_10),
    ("A.2.11", Queries.Query_A_2_11),
    ("A.2.12", Queries.Query_A_2_12),
    ("A.2.13", Queries.Query_A_2_13),

    // ─── Key-Value Single Queries (A.4.x → Redis üzerinden) ────
    ("A.4.1", Queries.Query_A_4_1),
    ("A.4.2", Queries.Query_A_4_2),
    ("A.4.3", Queries.Query_A_4_3),
    ("A.4.4", Queries.Query_A_4_4),
    ("A.4.5", Queries.Query_A_4_5),
    ("A.4.6", Queries.Query_A_4_6),
    ("A.4.7", Queries.Query_A_4_7),
    ("A.4.8", Queries.Query_A_4_8),
    ("A.4.9", Queries.Query_A_4_9),
    ("A.4.10", Queries.Query_A_4_10),

    // ─── Document-Search Single Queries (A.6.x → Elasticsearch) ─
    ("A.6.1", Queries.Query_A_6_1),
    ("A.6.2", Queries.Query_A_6_2),
    ("A.6.3", Queries.Query_A_6_3),
    ("A.6.4", Queries.Query_A_6_4),
    ("A.6.5", Queries.Query_A_6_5),

    // ─── Compound Polystore Queries (A.8.x → Multiple stores) ───
    ("A.8.1", Queries.Query_A_8_1),
    ("A.8.2", Queries.Query_A_8_2),
    ("A.8.3", Queries.Query_A_8_3),
    ("A.8.4", Queries.Query_A_8_4),
    ("A.8.5", Queries.Query_A_8_5),
    ("A.8.6", Queries.Query_A_8_6),
    ("A.8.7", Queries.Query_A_8_7)
  )

  def queryIdOf(phase: String, qName: String, run: Int): String =
    s"$phase|$qName|$run"

  def main(args: Array[String]): Unit = {

    val ipAddress = if (args.isDefinedAt(0)) args(0) else "127.0.0.1"
    val port = if (args.isDefinedAt(1)) args(1) else "2553"

    println("=" * 70)
    println("  Deney-4: Latency Breakdown Benchmark")
    println("=" * 70)
    println(s"  Sorgular:        ${compoundQueries.size} compound (A.8.1 - A.8.7)")
    println(s"  Warm-up runs:    $WARMUP_RUNS per query")
    println(s"  Measure runs:    $MEASURE_RUNS per query")
    println(s"  Toplam:          ${compoundQueries.size * (WARMUP_RUNS + MEASURE_RUNS) + 3} run")
    println("=" * 70)

    val sysConfig = ConfigFactory.parseString(s"akka.remote.artery.canonical.hostname = $ipAddress")
      .withFallback(ConfigFactory.parseString(s"akka.remote.artery.canonical.port = $port"))
      .withFallback(ConfigFactory.load("agent.conf"))

    val system = ActorSystem("LatencyBreakdownBenchmark", sysConfig)
    implicit val ec: ExecutionContext = system.dispatcher

    val sharedClusterClient = system.actorOf(
      ClusterClient.props(ClusterClientSettings(system)),
      "shared-cluster-client"
    )
    Thread.sleep(2000)
    println("  ClusterClient hazır.\n")

    // ─── Sistem warm-up
    println("[Sistem Warm-up] 3 sorgu...")
    for (i <- 1 to 3) {
      val (qName, qMap) = compoundQueries(i % compoundQueries.size)
      val qid = queryIdOf("warmup-system", qName, i)
      val agent = system.actorOf(Agent.props(sharedClusterClient), s"SysWarmupAgent-$i")
      val res = Try(Await.result(agent ? PolyStoreQuery(qMap, qid), timeout.duration))
      agent ! PoisonPill
      println(s"  sistem-warmup $i/3 ($qName): ${if (res.isSuccess) "OK" else "HATA"}")
      Thread.sleep(1000)
    }
    println("  Sistem hazır.\n")

    // ─── Her sorgu için warm-up + measurement
    for ((qName, qMap) <- compoundQueries) {
      println(s"┌─── $qName ───┐")

      for (i <- 1 to WARMUP_RUNS) {
        val qid = queryIdOf("warmup", qName, i)
        val agent = system.actorOf(Agent.props(sharedClusterClient),
          s"WarmupAgent-${qName.replace(".", "_")}-$i")
        val t0 = System.nanoTime()
        Try(Await.result(agent ? PolyStoreQuery(qMap, qid), timeout.duration))
        val ms = (System.nanoTime() - t0) / 1e6
        agent ! PoisonPill
        println(f"    warm-up $i/$WARMUP_RUNS: ${ms}%.0f ms")
        Thread.sleep(2000)
      }

      for (i <- 1 to MEASURE_RUNS) {
        val qid = queryIdOf("measure", qName, i)
        val agent = system.actorOf(Agent.props(sharedClusterClient),
          s"MeasureAgent-${qName.replace(".", "_")}-$i")
        val t0 = System.nanoTime()
        val res = Try(Await.result(agent ? PolyStoreQuery(qMap, qid), timeout.duration))
        val ms = (System.nanoTime() - t0) / 1e6
        agent ! PoisonPill
        res match {
          case Success(_) => println(f"    measure $i/$MEASURE_RUNS: ${ms}%.0f ms ✓")
          case Failure(ex) => println(f"    measure $i/$MEASURE_RUNS: HATA - ${ex.getMessage}")
        }
        Thread.sleep(2000)
      }

      println(s"└${"─" * 16}┘\n")
    }

    println("=" * 70)
    println("  Benchmark tamamlandı.")
    println("  Cluster node'un çalışma dizininde 3 CSV oluşmuş olmalı:")
    println("    - federation_latency.csv")
    println("    - executor_latency.csv")
    println("    - join_latency.csv  (Deney-5'ten gelen logger)")
    println("=" * 70)

    sharedClusterClient ! PoisonPill
    Thread.sleep(1000)
    system.terminate()
    Await.result(system.whenTerminated, 30.seconds)
  }
}
