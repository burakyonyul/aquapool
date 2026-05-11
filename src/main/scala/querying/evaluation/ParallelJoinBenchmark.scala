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
 * Deney-5: Parallel GRACE Hash Join Scalability Benchmark
 * =====================================================================
 *
 * AMAÇ:
 *   AQuAPooL'un parallel hash join uygulamasının sequential baseline'a
 *   göre net hızlanma (speedup) sağladığını sayısal olarak göstermek.
 *
 * STRATEJİ:
 *   Aynı sorgu, aynı kod, fakat Akka dispatcher fork-join thread
 *   pool size'ı 1, 2, 4, 8, 16 olarak değişen ortamlarda çalıştırılır.
 *
 *   - Thread pool = 1  ≡  klasik sıralı GRACE hash join
 *   - Thread pool = 16 ≡  tam paralel uygulama
 *
 * ÖLÇÜM ve SORGU TANIMLAMA:
 *   PolyStoreQuery'nin senderPath alanı query_id olarak kullanılır.
 *   Format: "<phase>|<query>|<run>"
 *     phase ∈ {warmup-system, warmup, measure}
 *     query  ∈ {A.8.5, A.8.1, A.8.7}
 *     run    ∈ ardışık sayı
 *   Bu string Federator → ParallelJoinManager zincirinden geçerek
 *   join_timings.csv'ye query_id sütununa yazılır.
 *
 *   ÖNEMLİ: A.8.7 multi-stage olduğu için aynı query_id'yle birden
 *   fazla join satırı oluşabilir (her stage için bir tane). Analizde
 *   bunları (query_id) ile gruplayıp toplama yaparız.
 *
 * SORGU SEÇİMİ:
 *   Join-heavy compound sorgular:
 *     - A.8.5  (küçük result set,   düşük speedup beklentisi)
 *     - A.8.1  (orta result set,    orta speedup beklentisi)
 *     - A.8.7  (büyük multi-stage,  yüksek speedup beklentisi)
 *
 * ÇALIŞTIRMA:
 *   # Her thread pool seviyesi için cluster node'unu (App.scala)
 *   # ayrı sbt oturumunda yeniden başlatın:
 *
 *   # Terminal-1 (cluster node, uzak veya yerel):
 *   sbt -J-Xmx8g \
 *       -Daquapool.dispatcher.parallelism=1 \
 *       -Daquapool.join.log-file=join_threads_1.csv \
 *       "runMain querying.App 155.223.25.1 2551"
 *
 *   # Terminal-2 (benchmark istemci — sade):
 *   sbt -J-Xmx4g "runMain querying.evaluation.ParallelJoinBenchmark"
 *
 *   # ... 2, 4, 8, 16 için tekrarla. Her sefer yalnızca App.scala'nın
 *   # -D bayrakları değişir; benchmark komutu aynı kalır.
 *
 * BUCKET SIZE BONUS DENEY:
 *   Yetersiz speedup durumunda cluster node'u:
 *     -Daquapool.dispatcher.parallelism=16 \
 *     -Daquapool.join.bucket-size=50 \
 *     -Daquapool.join.log-file=join_threads_16_bucket50.csv
 *   ile başlatın; benchmark komutu yine aynı.
 *
 * ÖN KOŞUL:
 *   - AQuAPooL cluster node ayağa kalkmış olmalı
 *   - ParallelJoinManager.scala'nın v2 hâli (queryId parametreli) ve
 *     Federator.scala'nın v2 hâli (queryId iletici) derlenmiş olmalı
 *   - application.conf'a default-dispatcher ve aquapool.join blokları
 *     eklenmiş olmalı (parallelism-factor = 1.0 KRİTİK)
 * =====================================================================
 */
object ParallelJoinBenchmark {

  implicit val timeout: Timeout = Timeout(15.minutes)

  val WARMUP_RUNS  = 3
  val MEASURE_RUNS = 10

  // Join-heavy compound sorgular
  val joinHeavyQueries: Seq[(String, Map[String, String])] = Seq(
    ("A.8.5", Queries.Query_A_8_5),  // Redis + PG, küçük result set
    ("A.8.1", Queries.Query_A_8_1),  // Redis + InfluxDB, orta result set
    ("A.8.7", Queries.Query_A_8_7)   // Redis + PG + ES + InfluxDB (multi-stage)
  )

  /** query_id formatı: "<phase>|<query>|<run>"
   *  Pipe ayraç olarak seçildi çünkü sorgu adlarında (A.8.5) nokta var,
   *  benchmark'ta `-` zaten Agent isimlendirmesinde kullanılıyor.
   */
  def queryIdOf(phase: String, qName: String, run: Int): String =
    s"$phase|$qName|$run"

  def main(args: Array[String]): Unit = {

    val ipAddress = if (args.isDefinedAt(0)) args(0) else "127.0.0.1"
    val port      = if (args.isDefinedAt(1)) args(1) else "2553"

    println("=" * 70)
    println("  Deney-5: Parallel Join Scalability Benchmark")
    println("=" * 70)
    println("  NOT: Bu istemci tarafıdır. Dispatcher parametreleri")
    println("       App.scala (cluster node) çalıştırılırken verilmeli.")
    println("       Aşağıdaki değerler istemci JVM'in default'larıdır —")
    println("       cluster node'un gerçek değerleri farklı olabilir.")
    println("=" * 70)

    val cfg = ConfigFactory.load()
    val parallelismLocal =
      if (cfg.hasPath("akka.actor.default-dispatcher.fork-join-executor.parallelism-min"))
        cfg.getInt("akka.actor.default-dispatcher.fork-join-executor.parallelism-min")
      else -1
    println(s"  [client-only]    dispatcher parallelism: $parallelismLocal")
    println(s"  warm-up runs:    $WARMUP_RUNS per query")
    println(s"  measure runs:    $MEASURE_RUNS per query")
    println(s"  total queries:   ${joinHeavyQueries.size}")
    println("=" * 70)

    // Actor system başlat (ScalabilityBenchmark ile aynı pattern)
    val sysConfig = ConfigFactory.parseString(s"akka.remote.artery.canonical.hostname = $ipAddress")
      .withFallback(ConfigFactory.parseString(s"akka.remote.artery.canonical.port = $port"))
      .withFallback(ConfigFactory.load("agent.conf"))

    val system = ActorSystem("ParallelJoinBenchmark", sysConfig)
    implicit val ec: ExecutionContext = system.dispatcher

    val sharedClusterClient = system.actorOf(
      ClusterClient.props(ClusterClientSettings(system)),
      "shared-cluster-client"
    )
    Thread.sleep(2000)
    println("  Paylaşımlı ClusterClient oluşturuldu.\n")

    // ─── Genel sistem warm-up: JIT/cache ısınması
    println("[Sistem Warm-up] JIT/cache ısıtması için 3 sorgu...")
    for (i <- 1 to 3) {
      val (qName, qMap) = joinHeavyQueries(i % joinHeavyQueries.size)
      val qid = queryIdOf("warmup-system", qName, i)
      val warmupAgent = system.actorOf(Agent.props(sharedClusterClient), s"SysWarmupAgent-$i")
      val result = Try(Await.result(
        warmupAgent ? PolyStoreQuery(qMap, qid),
        timeout.duration
      ))
      warmupAgent ! PoisonPill
      println(s"  sistem-warmup $i/3 ($qName, qid=$qid): " +
        (if (result.isSuccess) "OK" else "HATA"))
      Thread.sleep(1000)
    }
    println("  Sistem hazır.\n")

    // ─── Her sorgu için warm-up + measurement
    for ((qName, qMap) <- joinHeavyQueries) {

      println(s"┌─── Sorgu: $qName ───┐")

      // Sorguya özel warm-up
      println(f"  [warm-up: $WARMUP_RUNS run] (CSV'ye yazılır; query_id'de 'warmup' var → analizde filtrelenir)")
      for (i <- 1 to WARMUP_RUNS) {
        val qid = queryIdOf("warmup", qName, i)
        val agent = system.actorOf(Agent.props(sharedClusterClient), s"WarmupAgent-${qName.replace(".","_")}-$i")
        val t0 = System.nanoTime()
        Try(Await.result(agent ? PolyStoreQuery(qMap, qid), timeout.duration))
        val elapsedMs = (System.nanoTime() - t0) / 1e6
        agent ! PoisonPill
        println(f"    warm-up $i/$WARMUP_RUNS (qid=$qid): ${elapsedMs}%.0f ms wall-clock")
        Thread.sleep(2000)
      }

      // Measurement run'ları
      println(f"  [measure: $MEASURE_RUNS run] (CSV'ye yazılır; query_id'de 'measure' var → analizde alınır)")
      for (i <- 1 to MEASURE_RUNS) {
        val qid = queryIdOf("measure", qName, i)
        val agent = system.actorOf(Agent.props(sharedClusterClient), s"MeasureAgent-${qName.replace(".","_")}-$i")
        val t0 = System.nanoTime()
        val res = Try(Await.result(agent ? PolyStoreQuery(qMap, qid), timeout.duration))
        val elapsedMs = (System.nanoTime() - t0) / 1e6
        agent ! PoisonPill
        res match {
          case Success(_) =>
            println(f"    measure $i/$MEASURE_RUNS (qid=$qid): ${elapsedMs}%.0f ms wall-clock ✓")
          case Failure(ex) =>
            println(f"    measure $i/$MEASURE_RUNS (qid=$qid): HATA - ${ex.getMessage}")
        }
        Thread.sleep(2000)
      }

      println(s"└${"─" * 32}┘\n")
    }

    // ─── Kapatma
    println("=" * 70)
    println("  Benchmark tamamlandı.")
    println("  Cluster node'un çalışma dizininde join_threads_*.csv'yi kontrol edin.")
    println("  CSV'de query_id sütunundaki etiket kullanılarak:")
    println("    - 'warmup-system|*' satırları analiz dışı bırakılır")
    println("    - 'warmup|*' satırları analiz dışı bırakılır")
    println("    - 'measure|<query>|<run>' satırları kullanılır")
    println("=" * 70)

    sharedClusterClient ! PoisonPill
    Thread.sleep(1000)

    system.terminate()
    Await.result(system.whenTerminated, 30.seconds)
  }
}