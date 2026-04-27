package querying.evaluation

import querying.main.stores.PostgresqlStore

import java.io.{FileWriter, PrintWriter}
import java.sql.{Connection, ResultSet}
import scala.collection.mutable.ArrayBuffer

/**
 * PostgreSQL Benchmark Runner for AQuAPooL Evaluation
 *
 * Tüm 35 benchmark sorgusunu (28 single + 7 compound) PostgreSQL üzerinde çalıştırır.
 * Her sorgu: 3 warm-up + 10 ölçüm = 13 çalıştırma
 * Sonuçlar CSV dosyasına yazılır.
 *
 * Kullanım:
 * sbt "runMain PostgresBenchmark"
 * veya
 * scala PostgresBenchmark.scala
 *
 * Çıktı: pg_benchmark_results.csv
 */
object PostgresBenchmark {

  // ─── Bağlantı Ayarları ─────────────────────────────────────────────
  val DB_URL = "jdbc:postgresql://155.223.25.1:5433/mimic" // kendi ayarına göre değiştir
  val DB_USER = "bigdata" // kendi kullanıcı adın
  val DB_PASS = "postgres" // kendi şifren

  val WARMUP_RUNS = 3
  val MEASURE_RUNS = 10
  val TOTAL_RUNS = WARMUP_RUNS + MEASURE_RUNS
  val OUTPUT_FILE = "pg_benchmark_results.csv"

  // ─── Sorgu Tanımları ───────────────────────────────────────────────
  // Her tuple: (queryId, queryCategory, sqlString)

  val timeSeriesQueries: Seq[(String, String, String)] = Seq(
    ("A.1.1", "TS",
      Queries.Query_A_1_1.keys.head.stripMargin),

    ("A.1.2", "TS",
      Queries.Query_A_1_2.keys.head.stripMargin),

    ("A.1.3", "TS",
      Queries.Query_A_1_3.keys.head.stripMargin),

    ("A.1.4", "TS",
      Queries.Query_A_1_4.keys.head.stripMargin),

    ("A.1.5", "TS",
      Queries.Query_A_1_5.keys.head.stripMargin),

    ("A.1.6", "TS",
      Queries.Query_A_1_6.keys.head.stripMargin),

    ("A.1.7", "TS",
      Queries.Query_A_1_7.keys.head.stripMargin),

    ("A.1.8", "TS",
      Queries.Query_A_1_8.keys.head.stripMargin),

    ("A.1.9", "TS",
      Queries.Query_A_1_9.keys.head.stripMargin),

    ("A.1.10", "TS",
      Queries.Query_A_1_10.keys.head.stripMargin),

    ("A.1.11", "TS",
      Queries.Query_A_1_11.keys.head.stripMargin),

    ("A.1.12", "TS",
      Queries.Query_A_1_12.keys.head.stripMargin),

    ("A.1.13", "TS",
      Queries.Query_A_1_13.keys.head.stripMargin)
  )

  val keyValueQueries: Seq[(String, String, String)] = Seq(
    ("A.3.1", "KV",
      Queries.Query_A_3_1.keys.head.stripMargin),

    ("A.3.2", "KV",
      Queries.Query_A_3_2.keys.head.stripMargin),

    ("A.3.3", "KV",
      Queries.Query_A_3_3.keys.head.stripMargin),

    ("A.3.4", "KV",
      Queries.Query_A_3_4.keys.head.stripMargin),

    ("A.3.5", "KV",
      Queries.Query_A_3_5.keys.head.stripMargin),

    ("A.3.6", "KV",
      Queries.Query_A_3_6.keys.head.stripMargin),

    ("A.3.7", "KV",
      Queries.Query_A_3_7.keys.head.stripMargin),

    ("A.3.8", "KV",
      Queries.Query_A_3_8.keys.head.stripMargin),

    ("A.3.9", "KV",
      Queries.Query_A_3_9.keys.head.stripMargin),

    ("A.3.10", "KV",
      Queries.Query_A_3_10.keys.head.stripMargin)
  )

  val documentSearchQueries: Seq[(String, String, String)] = Seq(
    ("A.5.1", "DS",
      Queries.Query_A_5_1.keys.head.stripMargin),

    ("A.5.2", "DS",
      Queries.Query_A_5_2.keys.head.stripMargin),

    ("A.5.3", "DS",
      Queries.Query_A_5_3.keys.head.stripMargin),

    ("A.5.4", "DS",
      Queries.Query_A_5_4.keys.head.stripMargin),

    ("A.5.5", "DS",
      Queries.Query_A_5_5.keys.head.stripMargin)
  )

  val compoundQueries: Seq[(String, String, String)] = Seq(
    ("A.7.1", "CQ",
      Queries.Query_A_7_1.keys.head.stripMargin),

    ("A.7.2", "CQ",
      Queries.Query_A_7_2.keys.head.stripMargin),

    ("A.7.3", "CQ",
      Queries.Query_A_7_3.keys.head.stripMargin),

    ("A.7.4", "CQ",
      Queries.Query_A_7_4.keys.head.stripMargin),

    ("A.7.5", "CQ",
      Queries.Query_A_7_5.keys.head.stripMargin),

    ("A.7.6", "CQ",
      Queries.Query_A_7_6.keys.head.stripMargin),

    ("A.7.7", "CQ",
      Queries.Query_A_7_7.keys.head.stripMargin)
  )

  // ─── Yardımcı Fonksiyonlar ─────────────────────────────────────────

  /** ResultSet'in tüm satırlarını consume eder ve satır sayısını döner. */
  def consumeResultSet(rs: ResultSet): Int = {
    val colCount = rs.getMetaData.getColumnCount
    var rowCount = 0
    while (rs.next()) {
      // Tüm sütunları oku — sadece consume etmek için
      var col = 1
      while (col <= colCount) {
        rs.getObject(col)
        col += 1
      }
      rowCount += 1
    }
    rowCount
  }

  /** Tek bir sorguyu çalıştırır ve (elapsed_ns, rowCount) döner. */
  def executeAndMeasure(conn: Connection, sql: String): (Long, Int) = {
    val stmt = conn.createStatement()
    val startTime = System.nanoTime()
    val rs = stmt.executeQuery(sql)
    val rowCount = consumeResultSet(rs)
    val elapsedNs = System.nanoTime() - startTime
    rs.close()
    stmt.close()
    (elapsedNs, rowCount)
  }

  /** PostgreSQL session cache temizleme */
  def clearSessionCache(conn: Connection): Unit = {
    val stmt = conn.createStatement()
    stmt.execute("DISCARD ALL;")
    stmt.close()
  }

  // ─── Ana Program ──────────────────────────────────────────────────

  def main(args: Array[String]): Unit = {
    // Tüm sorguları birleştir
    val allQueries = timeSeriesQueries ++ keyValueQueries ++ documentSearchQueries ++ compoundQueries

    println(s"PostgreSQL Benchmark Runner")
    println(s"Total queries: ${allQueries.size}")
    println(s"Runs per query: $WARMUP_RUNS warm-up + $MEASURE_RUNS measured = $TOTAL_RUNS")
    println(s"Output file: $OUTPUT_FILE")
    println("=" * 70)

    // Bağlantıyı aç — tüm deney boyunca açık kalacak
    val conn = PostgresqlStore.hikariDataSource.getConnection
    println(s"Connected to: $DB_URL")

    // CSV dosyasını aç
    val csvWriter = new PrintWriter(new FileWriter(OUTPUT_FILE))
    csvWriter.println("system,queryId,category,run,elapsed_ns,elapsed_ms,row_count")

    for ((queryId, category, sql) <- allQueries) {
      println(s"\n--- $queryId ($category) ---")

      // Bu sorgu grubunun başında cache temizle
      clearSessionCache(conn)

      val measuredTimes = new ArrayBuffer[Long]()
      var lastRowCount = 0

      for (runNum <- 1 to TOTAL_RUNS) {
        val (elapsedNs, rowCount) = executeAndMeasure(conn, sql)
        val elapsedMs = elapsedNs / 1000000.0
        lastRowCount = rowCount

        val runType = if (runNum <= WARMUP_RUNS) "WARMUP" else "MEASURE"
        println(f"  Run $runNum%2d [$runType%7s]: $elapsedMs%12.2f ms ($rowCount rows)")

        if (runNum > WARMUP_RUNS) {
          // Sadece ölçüm run'larını CSV'ye yaz
          csvWriter.println(s"PG,$queryId,$category,$runNum,$elapsedNs,${elapsedMs.toLong},$rowCount")
          measuredTimes += elapsedNs
        }
      }

      // Bu sorgunun istatistiklerini hesapla ve ekrana bas
      val timesMs = measuredTimes.map(_ / 1000000.0)
      val n = timesMs.size
      val mean = timesMs.sum / n
      val std = math.sqrt(timesMs.map(t => math.pow(t - mean, 2)).sum / (n - 1))
      val ci = 2.262 * std / math.sqrt(n)

      println(f"  Summary: mean=$mean%12.2f ms, std=$std%10.2f ms, 95%% CI=[${mean - ci}%12.2f, ${mean + ci}%12.2f] ms, rows=$lastRowCount")
    }

    csvWriter.close()
    conn.close()

    println("\n" + "=" * 70)
    println(s"Done. Results saved to: $OUTPUT_FILE")
    println("Bu CSV dosyasını Python istatistik scripti ile işleyebilirsiniz.")
  }
}
