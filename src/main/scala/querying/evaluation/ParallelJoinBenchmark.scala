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
 * ====================================================================
 * Parallel GRACE Hash Join Scalability Benchmark
 * ======================================================================== *
 * OBJECTIVE:
 * To numerically demonstrate that AQuAPooL's parallel hash join implementation provides a net speedup compared to the sequential baseline.
 * *
 * STRATEGY:
 * The same query, the same code, but Akka dispatcher fork-join thread
 * is run in environments where the pool size varies from 1, 2, 4, 8, to 16.
 *
 * - Thread pool = 1 ≡ classic sequential GRACE hash join
 * - Thread pool = 16 ≡ fully parallel implementation
 *
 * MEASUREMENT and QUERY DEFINITION:
 * The senderPath field of PolyStoreQuery is used as query_id.
 * Format: "<phase>|<query>|<run>"
 * phase ∈ {warmup-system, warmup, measure}
 * query ∈ {A.8.5, A.8.1, A.8.7}
 * run ∈ sequential number
 * This string passes through the Federationator → ParallelJoinManager chain
 * and is written to the query_id column in join_timings.csv.
 *
 * IMPORTANT: Since A.8.7 is multi-stage, more than one join row can be created with the same query_id (one for each stage). In the analysis, we group these (query_id) and perform a summation. *
 * QUERY SELECTION:
 * Join-heavy compound queries:
 * - A.8.5 (small result set, low speedup expectation)
 * - A.8.1 (medium result set, medium speedup expectation)
 * - A.8.7 (large multi-stage, high speedup expectation)
 *
 * EXECUTION:
 * # Restart the cluster node (App.scala) for each thread pool level
 * # in a separate sbt session:
 *
 * # Terminal-1 (cluster node, remote or local):
 * sbt -J-Xmx8g
 * -Daquapool.dispatcher.parallelism=1
 * -Daquapool.join.log-file=join_threads_1.csv
 * "runMain querying.App 155.223.25.1 2551"
 *
 * # Terminal-2 (benchmark client — plain):
 * sbt -J-Xmx4g "runMain querying.evaluation.ParallelJoinBenchmark"
 *
 * # ... Repeat for 2, 4, 8, 16. Each time only the App.scala
 * # -D flags change; the benchmark command remains the same. *
 * BUCKET SIZE BONUS EXPERIMENT:
 * In case of insufficient speedup, start the cluster node with:
 * -Daquapool.dispatcher.parallelism=16 \
 * -Daquapool.join.bucket-size=50 \
 * -Daquapool.join.log-file=join_threads_16_bucket50.csv
 * ; the benchmark command remains the same. *
 * PREREQUISITE:
 * - The AQuAPooL cluster node must be up and running.
 * - Version 2 of ParallelJoinManager.scala (with queryId parameter) and
 * Version 2 of Federator.scala (queryId forwarder) must be compiled.
 * - The default-dispatcher and aquapool.join blocks must be added to application.conf (parallelism-factor = 1.0 CRITICAL).
 * ======================================================================
 */
object ParallelJoinBenchmark {

  implicit val timeout: Timeout = Timeout(15.minutes)

  val WARMUP_RUNS  = 3
  val MEASURE_RUNS = 10

  // Join-heavy compound sorgular
  val joinHeavyQueries = Seq(
    ("A.8.5", Queries.Query_A_8_5),  // Redis + PG, küçük result set
    ("A.8.1", Queries.Query_A_8_1),  // Redis + InfluxDB, orta result set
    ("A.8.7", Queries.Query_A_8_7)   // Redis + PG + ES + InfluxDB (multi-stage)
  )

  /** query_id format: "<phase>|<query>|<run>"
   * Pipe was chosen as a separator because query names (A.8.5) contain periods,
   * `-` is already used in Agent naming in the benchmark. */
  def queryIdOf(phase: String, qName: String, run: Int) =
    s"$phase|$qName|$run"

  def main(args: Array[String]): Unit = {

    val ipAddress = if (args.isDefinedAt(0)) args(0) else "127.0.0.1"
    val port      = if (args.isDefinedAt(1)) args(1) else "2553"

    println("=" * 70)
    println("  Parallel Join Scalability Benchmark")
    println("=" * 70)
    println("  NOTE: This is the client side. Dispatcher parameters.")
    println("       This information must be provided when running App.scala (cluster node).")
    println("       The following values are the client JVM defaults. —")
    println("       The actual values of the cluster node may be different..")
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

    // Start the actor system (same pattern as Scalability Benchmark)
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
    println("  A shared cluster client was created..\n")

    // ─── Genel sistem warm-up: JIT/cache ısınması
    println("[System Warm-up] 3 queries for JIT/cache warm-up...")
    for (i <- 1 to 3) {
      val (qName, qMap) = joinHeavyQueries(i % joinHeavyQueries.size)
      val qid = queryIdOf("warmup-system", qName, i)
      val warmupAgent = system.actorOf(Agent.props(sharedClusterClient), s"SysWarmupAgent-$i")
      val result = Try(Await.result(
        warmupAgent ? PolyStoreQuery(qMap, qid),
        timeout.duration
      ))
      warmupAgent ! PoisonPill
      println(s"  system-warmup $i/3 ($qName, qid=$qid): " +
        (if (result.isSuccess) "OK" else "HATA"))
      Thread.sleep(1000)
    }
    println("  System is ready.\n")

    // ─── Warm-up and measurement for each query.
    for ((qName, qMap) <- joinHeavyQueries) {

      println(s"┌─── Query: $qName ───┐")

      // Interrogation-specific warm-up
      println(f"  [warm-up: $WARMUP_RUNS run] (It is written to a CSV; query_id has 'warmup' → it is filtered in the analysis.)")
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

      // Measurement runs
      println(f"  [measure: $MEASURE_RUNS run] (It is written to a CSV; query_id contains 'measure' → it is retrieved in the analysis.)")
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
    println("  Benchmark completed.")
    println("  Check the join_threads_*.csv file in the cluster node's working directory.")
    println("  Using the tag in the query_id column in the CSV:")
    println("    - Lines containing 'warmup-system|*' are excluded from analysis.")
    println("    - Lines containing 'warmup|*' are excluded from analysis.")
    println("    - The lines 'measure|<query>|<run>' are used.")
    println("=" * 70)

    sharedClusterClient ! PoisonPill
    Thread.sleep(1000)

    system.terminate()
    Await.result(system.whenTerminated, 30.seconds)
  }
}