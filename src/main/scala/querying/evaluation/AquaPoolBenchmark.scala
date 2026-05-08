package querying.evaluation

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import querying.actor.Agent
import querying.message.{PolyStoreQuery, Result}

import java.io.{FileWriter, PrintWriter}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}

/**
 * AQuAPooL Benchmark Runner
 *
 * Tüm 35 benchmark sorgusunu AQuAPooL polystore üzerinden çalıştırır.
 * Her sorgu: 3 warm-up + 10 ölçüm = 13 çalıştırma
 * Sonuçlar CSV dosyasına yazılır.
 *
 * ask pattern ile senkron bekleme yapılır:
 *   - agent ? PolyStoreQuery(...) → Future döner
 *   - Await.result ile sonuç beklenir
 *   - Zamanlama: mesaj gönderiminden sonuç alımına kadar
 *
 * Kullanım:
 * sbt "runMain querying.AquaPoolBenchmark"
 * veya
 * sbt "runMain querying.AquaPoolBenchmark 192.168.1.10 2553"
 */
object AquaPoolBenchmark {

  val WARMUP_RUNS = 3
  val MEASURE_RUNS = 10
  val TOTAL_RUNS = WARMUP_RUNS + MEASURE_RUNS
  val OUTPUT_FILE = "aq_benchmark_results.csv"

  // Timeout: en uzun sorgu için yeterli olmalı.
  // Time-series sorgular uzun sürebilir (A.2.1 gibi),
  // compound sorgular da uzun sürebilir.
  // Güvenli bir değer olarak 10 dakika ayarlandı.
  implicit val timeout: Timeout = Timeout(10.minutes)

  def main(args: Array[String]): Unit = {
    val ipAddress = if (args.isDefinedAt(0)) args(0) else "127.0.0.1"
    val port = if (args.isDefinedAt(1)) args(1) else "2553"

    val config = ConfigFactory.parseString(s"akka.remote.artery.canonical.hostname = $ipAddress")
      .withFallback(ConfigFactory.parseString(s"akka.remote.artery.canonical.port = $port"))
      .withFallback(ConfigFactory.load("agent.conf"))

    val system = ActorSystem("BenchmarkRunner", config)
    val agent = system.actorOf(Agent.props, "BenchmarkAgent")

    // ─── Sorgu Tanımları ─────────────────────────────────────────
    // Queries object'inden sorgu sabitlerini kullan.
    // Aşağıdaki mapping'i kendi Queries.scala dosyandaki sabitlere göre güncelle.
    //
    // Format: (queryId, category, querySabit)
    // queryId: CSV'de ve tablolarda kullanılacak isim
    // category: TS=TimeSeries, KV=KeyValue, DS=DocSearch, CQ=Compound
    // querySabit: Queries object'indeki HashMap veya sorgu objesi

    val allQueries: Seq[(String, String, Map[String, String])] = Seq(
      // ─── Time-Series Single Queries (A.2.x → InfluxDB üzerinden) ───
      ("A.2.1", "TS", Queries.Query_A_2_1),
      ("A.2.2", "TS", Queries.Query_A_2_2),
      ("A.2.3", "TS", Queries.Query_A_2_3),
      ("A.2.4", "TS", Queries.Query_A_2_4),
      ("A.2.5", "TS", Queries.Query_A_2_5),
      ("A.2.6", "TS", Queries.Query_A_2_6),
      ("A.2.7", "TS", Queries.Query_A_2_7),
      ("A.2.8", "TS", Queries.Query_A_2_8),
      ("A.2.9", "TS", Queries.Query_A_2_9),
      ("A.2.10", "TS", Queries.Query_A_2_10),
      ("A.2.11", "TS", Queries.Query_A_2_11),
      ("A.2.12", "TS", Queries.Query_A_2_12),
      ("A.2.13", "TS", Queries.Query_A_2_13),

      // ─── Key-Value Single Queries (A.4.x → Redis üzerinden) ────
      ("A.4.1", "KV", Queries.Query_A_4_1),
      ("A.4.2", "KV", Queries.Query_A_4_2),
      ("A.4.3", "KV", Queries.Query_A_4_3),
      ("A.4.4", "KV", Queries.Query_A_4_4),
      ("A.4.5", "KV", Queries.Query_A_4_5),
      ("A.4.6", "KV", Queries.Query_A_4_6),
      ("A.4.7", "KV", Queries.Query_A_4_7),
      ("A.4.8", "KV", Queries.Query_A_4_8),
      ("A.4.9", "KV", Queries.Query_A_4_9),
      ("A.4.10", "KV", Queries.Query_A_4_10),

      // ─── Document-Search Single Queries (A.6.x → Elasticsearch) ─
      ("A.6.1", "DS", Queries.Query_A_6_1),
      ("A.6.2", "DS", Queries.Query_A_6_2),
      ("A.6.3", "DS", Queries.Query_A_6_3),
      ("A.6.4", "DS", Queries.Query_A_6_4),
      ("A.6.5", "DS", Queries.Query_A_6_5),

      // ─── Compound Polystore Queries (A.8.x → Multiple stores) ───
      ("A.8.1", "CQ", Queries.Query_A_8_1),
      ("A.8.2", "CQ", Queries.Query_A_8_2),
      ("A.8.3", "CQ", Queries.Query_A_8_3),
      ("A.8.4", "CQ", Queries.Query_A_8_4),
      ("A.8.5", "CQ", Queries.Query_A_8_5),
      ("A.8.6", "CQ", Queries.Query_A_8_6),
      ("A.8.7", "CQ", Queries.Query_A_8_7)
    )

    println(s"AQuAPooL Benchmark Runner")
    println(s"Total queries: ${allQueries.size}")
    println(s"Runs per query: $WARMUP_RUNS warm-up + $MEASURE_RUNS measured = $TOTAL_RUNS")
    println(s"Output file: $OUTPUT_FILE")
    println("=" * 70)

    val csvWriter = new PrintWriter(new FileWriter(OUTPUT_FILE))
    csvWriter.println("system,queryId,category,run,elapsed_ns,elapsed_ms,row_count")

    for ((queryId, category, queryObj) <- allQueries) {
      println(s"\n--- $queryId ($category) ---")

      val measuredTimes = new ArrayBuffer[Long]()

      for (runNum <- 1 to TOTAL_RUNS) {

        val startTime = System.nanoTime()

        // ask pattern: Future döner, Await ile bekle
        val future = agent ? PolyStoreQuery(queryObj, s"${runNum}")
        val result = Try(Await.result(future, timeout.duration))

        val elapsedNs = System.nanoTime() - startTime

        // Sonuçtan satır sayısını çıkar
        // NOT: Burayı kendi result tipine göre güncelle.
        // Federator'ın döndürdüğü mesaj tipine bağlı olarak
        // row count'u çıkarman gerekebilir.
        val rowCount = result match {
          case Success(r) => extractRowCount(r)
          case Failure(ex) =>
            println(s"  ERROR in run $runNum: ${ex.getMessage}")
            -1
        }

        val elapsedMs = elapsedNs / 1000000.0
        val runType = if (runNum <= WARMUP_RUNS) "WARMUP" else "MEASURE"
        println(f"  Run $runNum%2d [$runType%7s]: $elapsedMs%12.2f ms ($rowCount rows)")

        if (runNum > WARMUP_RUNS) {
          csvWriter.println(s"AQ,$queryId,$category,$runNum,$elapsedNs,${elapsedMs.toLong},$rowCount")
          measuredTimes += elapsedNs
        }
      }

      // İstatistikleri hesapla
      if (measuredTimes.nonEmpty) {
        val timesMs = measuredTimes.map(_ / 1000000.0)
        val n = timesMs.size
        val mean = timesMs.sum / n
        val std = math.sqrt(timesMs.map(t => math.pow(t - mean, 2)).sum / (n - 1))
        val ci = 2.262 * std / math.sqrt(n)
        println(f"  Summary: mean=$mean%12.2f ms, std=$std%10.2f ms, 95%% CI=[${mean - ci}%12.2f, ${mean + ci}%12.2f] ms")
      }
    }

    csvWriter.close()

    println("\n" + "=" * 70)
    println(s"Done. Results saved to: $OUTPUT_FILE")
    println("Shutting down actor system...")

    system.terminate()
    Await.result(system.whenTerminated, 30.seconds)
    println("Actor system terminated.")
  }

  /**
   * Federator'dan dönen sonuç mesajından satır sayısını çıkar.
   *
   * NOT: Bu fonksiyonu kendi result tipine göre güncelle.
   * Federator'ın döndürdüğü mesaj tipi ne ise (örn. List, Seq,
   * PolyStoreResult, vb.) ona göre pattern match yap.
   *
   * Örnekler:
   * case list: List[_] => list.size
   * case result: PolyStoreResult => result.rows.size
   * case map: Map[_, _] => map.size
   * case seq: Seq[_] => seq.size
   */
  private def extractRowCount(result: Any): Int = {
    result match {
      case result: Result =>
        val resultSet = result.toResultSet
        var rows = 0
        while (resultSet.hasNext) {
          resultSet.next()
          rows += 1
        }
        rows
      case other =>
        println(s"  [WARN] Unknown result type: ${other.getClass.getSimpleName}")
        -1
    }
  }
}
