package querying.main

import com.typesafe.config.ConfigFactory

import java.io.{FileWriter, PrintWriter}
import java.nio.file.{Files, Paths}

/**
 * =====================================================================
 * Deney-4: Latency Breakdown CSV Logger
 * =====================================================================
 *
 * İki ayrı log dosyasını yönetir:
 *
 * 1) executor_latency.csv — Her Executor (Redis/PG/InfluxDB/ES) için
 *    her sorguda iki fazı ayrı ayrı loglar:
 *      - store_exec_ms:  backend sorgusu süresi (DB içinde geçen zaman)
 *      - transform_ms:   backend response → polystore global RDF model
 *                        dönüşüm maliyeti
 *
 * 2) federation_latency.csv — Federator için her sorgunun toplam süresi
 *    ve dispatch süresi:
 *      - total_ms:       PolyStoreQuery alınması → final Result gönderimi
 *      - dispatch_ms:    PolyStoreQuery alınması → tüm Executor'lere
 *                        ExecuteQuery dağıtılması
 *
 * CSV dosyaları append modunda yazılır. Cluster node JVM'inin çalışma
 * dizininde oluşur (system property ile değiştirilebilir).
 *
 * Sistem property'leri:
 *   -Daquapool.latency.executor-log=/path/to/executor.csv
 *   -Daquapool.latency.federation-log=/path/to/federation.csv
 */
object LatencyLogger {

  private val executorLogPath: String = {
    val cfg = ConfigFactory.load()
    if (cfg.hasPath("aquapool.latency.executor-log"))
      cfg.getString("aquapool.latency.executor-log")
    else
      "executor_latency.csv"
  }

  private val federationLogPath: String = {
    val cfg = ConfigFactory.load()
    if (cfg.hasPath("aquapool.latency.federation-log"))
      cfg.getString("aquapool.latency.federation-log")
    else
      "federation_latency.csv"
  }

  // Thread-safe header tracking
  private val execLock = new Object()
  private val fedLock  = new Object()
  @volatile private var execHeaderWritten = false
  @volatile private var fedHeaderWritten  = false

  /** Executor-side: per-query iki faz (store + transform). */
  def logExecutorPhase(queryId: String, store: String,
                       storeExecMs: Double, transformMs: Double): Unit =
    execLock.synchronized {
      try {
        val fileExists = Files.exists(Paths.get(executorLogPath))
        val writer = new PrintWriter(new FileWriter(executorLogPath, true))
        if (!fileExists && !execHeaderWritten) {
          writer.println("timestamp_ms,query_id,store,store_exec_ms,transform_ms")
          execHeaderWritten = true
        }
        val safeQid = "\"" + queryId.replace("\"", "\"\"") + "\""
        writer.println(f"${System.currentTimeMillis()}," +
          f"$safeQid,$store,$storeExecMs%.3f,$transformMs%.3f")
        writer.close()
      } catch {
        case ex: Exception =>
          System.err.println(s"[LatencyLogger] executor CSV error: ${ex.getMessage}")
      }
    }

  /** Federator-side: per-query toplam + dispatch. */
  def logFederationPhase(queryId: String, totalMs: Double, dispatchMs: Double): Unit =
    fedLock.synchronized {
      try {
        val fileExists = Files.exists(Paths.get(federationLogPath))
        val writer = new PrintWriter(new FileWriter(federationLogPath, true))
        if (!fileExists && !fedHeaderWritten) {
          writer.println("timestamp_ms,query_id,total_ms,dispatch_ms")
          fedHeaderWritten = true
        }
        val safeQid = "\"" + queryId.replace("\"", "\"\"") + "\""
        writer.println(f"${System.currentTimeMillis()}," +
          f"$safeQid,$totalMs%.3f,$dispatchMs%.3f")
        writer.close()
      } catch {
        case ex: Exception =>
          System.err.println(s"[LatencyLogger] federation CSV error: ${ex.getMessage}")
      }
    }
}
