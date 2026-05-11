package querying.actor.join

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import com.hp.hpl.jena.query.{ResultSet, ResultSetFactory, ResultSetFormatter}
import com.hp.hpl.jena.sparql.core.Var
import com.hp.hpl.jena.sparql.engine.binding.Binding
import com.typesafe.config.ConfigFactory
import join.{MultipleNode, QueryIterCollection}
import play.api.libs.json.Json
import querying.message.{DistributeBuckets, PerformHashJoin, Result}

import java.io.{FileWriter, PrintWriter}
import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

object ParallelJoinManager {

  // ====================================================================
  // Deney-5: bucketSize artık application.conf'tan okunuyor.
  //
  // System property veya environment ile override edilebilir:
  //   sbt -Daquapool.join.bucket-size=50 "runMain ..."
  //
  // Config yoksa default 100 (orijinal davranış).
  // ====================================================================
  val bucketSize: Int = {
    val cfg = ConfigFactory.load()
    if (cfg.hasPath("aquapool.join.bucket-size"))
      cfg.getInt("aquapool.join.bucket-size")
    else
      100
  }

  // ====================================================================
  // Deney-5: Join phase timing & actor counting infrastructure
  //
  // Her ParallelJoinManager instance'ı kendi join phase'i için
  // toplam wall-clock süreyi ve spawn edilen HashJoinPerformer sayısını
  // ölçer. Sonuçlar CSV'ye append edilir.
  //
  // CSV path system property ile değiştirilebilir:
  //   -Daquapool.join.log-file=/path/to/file.csv
  // Default: ./join_timings.csv (çalışma dizini)
  //
  // CSV format:
  //   timestamp_ms, query_id, thread_pool_size, bucket_size,
  //   actors_spawned, first_rs_rows, second_rs_rows, join_time_ms
  // ====================================================================
  private val logFilePath: String = {
    val cfg = ConfigFactory.load()
    if (cfg.hasPath("aquapool.join.log-file"))
      cfg.getString("aquapool.join.log-file")
    else
      "join_timings.csv"
  }

  // Thread pool size'ı log'a yazmak için config'ten oku
  // (Akka çalışma zamanında bunu doğrudan vermez; biz config'ten alırız)
  private val threadPoolSize: Int = {
    val cfg = ConfigFactory.load()
    val path = "akka.actor.default-dispatcher.fork-join-executor.parallelism-min"
    if (cfg.hasPath(path)) cfg.getInt(path) else -1
  }

  // CSV header bir kez yazılır (process başına)
  private val csvLock = new Object()
  @volatile private var headerWritten = false

  def logJoinTiming(queryId: String, actorsSpawned: Int,
                    firstRows: Int, secondRows: Int,
                    joinTimeMs: Double): Unit = csvLock.synchronized {
    try {
      val fileExists = Files.exists(Paths.get(logFilePath))
      val writer = new PrintWriter(new FileWriter(logFilePath, true))  // append mode
      if (!fileExists && !headerWritten) {
        writer.println("timestamp_ms,query_id,thread_pool_size,bucket_size," +
          "actors_spawned,first_rs_rows,second_rs_rows,join_time_ms")
        headerWritten = true
      }
      // CSV-safe query_id: çift tırnak içine al, içindeki tırnakları kaçır
      val safeQueryId = "\"" + queryId.replace("\"", "\"\"") + "\""
      writer.println(f"${System.currentTimeMillis()}," +
        f"$safeQueryId,$threadPoolSize,$bucketSize,$actorsSpawned," +
        f"$firstRows,$secondRows,$joinTimeMs%.3f")
      writer.close()
    } catch {
      case ex: Exception =>
        // Logging hata verirse benchmark'ı patlatma; sadece konsola yaz
        System.err.println(s"[ParallelJoinManager] CSV log error: ${ex.getMessage}")
    }
  }

  // ====================================================================
  // Deney-5: queryId parametresi ile Props.
  //
  // Federator, child ParallelJoinManager'ı spawn ederken kendi
  // PolyStoreQuery'sindeki senderPath'i query identifier olarak iletir.
  // Bu sayede CSV'de hangi satırın hangi sorguya ait olduğu net olur.
  //
  // Parametresiz props() backward-compatible olarak korunur (eski
  // çağrılar için "unknown" etiketiyle çalışır).
  // ====================================================================
  def props: Props = Props(new ParallelJoinManager("unknown"))
  def props(queryId: String): Props = Props(new ParallelJoinManager(queryId))

  /*
    val extractEntityId: ShardRegion.ExtractEntityId = {
      case dbs@DistributeBuckets(_, _) => (dbs.hashCode.toString, dbs)
    }

    private val numberOfShards = 20

    val extractShardId: ShardRegion.ExtractShardId = {
      case dbs@DistributeBuckets(_, _) => (dbs.hashCode % numberOfShards).toString
    }
  */
}

class ParallelJoinManager(queryId: String) extends Actor with ActorLogging {

  private var bucketCount = 0
  private var bindings: Vector[Binding] = Vector.empty
  private var registeryList: Vector[ActorRef] = Vector.empty
  private var joinKey = 1

  // === Deney-5 instrumentation alanları ===
  private var joinStartNanos: Long = 0L
  private var actorsSpawned: Int = 0
  private var firstRsRows: Int = 0
  private var secondRsRows: Int = 0
  // ========================================

  override def preStart(): Unit = {
    super.preStart
    //MetricStoreUtils.increaseActorCount
  }

  override def postStop(): Unit = {
    super.postStop
    //MetricStoreUtils.decreaseActorCount
  }

  override def receive: Receive = {

    case DistributeBuckets(firstRes, secondRes) =>
      performDistribution(firstRes, secondRes)

    case result@Result(_, _, _) =>
      handleJoinResult(result)
    /*
        case ShardRegion.Passivate =>
          log.info("Passivation message has been received start parent shard!")
          context.stop(self)
    */
  }

  private def handleJoinResult(result: Result) = {
    bucketCount -= 1
    val resultSet = result.toResultSet
    insertResult(resultSet)
    // if join has completed notify join result
    if (bucketCount.hashCode() == 0) {
      // === Deney-5: join phase tamamlandı, süreyi ölç ve logla ===
      val joinEndNanos = System.nanoTime()
      val joinTimeMs = (joinEndNanos - joinStartNanos) / 1e6
      ParallelJoinManager.logJoinTiming(
        queryId, actorsSpawned, firstRsRows, secondRsRows, joinTimeMs
      )
      log.info(f"[Deney5] join phase complete: queryId=$queryId, actors=$actorsSpawned, " +
        f"firstRows=$firstRsRows, secondRows=$secondRsRows, time=${joinTimeMs}%.2f ms")
      // ===========================================================

      val finalResult = generateResult(resultSet.getResultVars.asScala, bindings)
      notifyRegisteryList(finalResult)
      context.stop(self)
      //context.parent ! ShardRegion.Passivate(stopMessage = PoisonPill)
    }
  }

  protected def performDistribution(firstRes: Result, secondRes: Result): Unit = {
    registerSender

    // === Deney-5: join phase başlangıç zamanı ===
    joinStartNanos = System.nanoTime()
    actorsSpawned = 0

    // find common vars between result sets
    val commonVars = findCommonVars(firstRes.resultVars, secondRes.resultVars)

    // === Deney-5: row count'ları logla (debug + analiz için) ===
    val firstMap = generateBucketMap(firstRes.toResultSet, commonVars)
    val secondMap = generateBucketMap(secondRes.toResultSet, commonVars)
    firstRsRows = firstMap.values.map(_.size).sum
    secondRsRows = secondMap.values.map(_.size).sum

    // get bucket iterators
    val bucketIterFirst = firstMap.values.iterator
    val bucketIterSecond = secondMap.values.iterator

    // iterate over bucket iterators and perform hash join
    while (bucketIterFirst.hasNext && bucketIterSecond.hasNext) {
      performHashJoin(firstRes.resultVars, secondRes.resultVars, bucketIterFirst, bucketIterSecond)
    }
  }

  def performHashJoin(varsFirst: Seq[String], varsSecond: Seq[String],
                      bucketIterFirst: Iterator[Vector[Binding]],
                      bucketIterSecond: Iterator[Vector[Binding]]): Unit = {
    bucketCount += 1
    actorsSpawned += 1  // === Deney-5: aktör sayacı ===
    val resultFirst = generateResult(varsFirst, bucketIterFirst.next)
    val resultSecond = generateResult(varsSecond, bucketIterSecond.next)
    val hashJoinPerformer = context.actorOf(HashJoinPerformer.props)
    hashJoinPerformer ! PerformHashJoin(resultFirst, resultSecond)
  }

  private def generateResult(vars: Seq[String], bucket: Vector[Binding]): Result = {
    val outputStream = new java.io.ByteArrayOutputStream
    ResultSetFormatter.outputAsJSON(outputStream, ResultSetFactory.create(new QueryIterCollection(bucket.asJava), vars.asJava))
    Result(Json.parse(outputStream.toByteArray), vars, joinKey)
  }

  def findCommonVars(varsFirst: Seq[String], varsSecond: Seq[String]): Vector[String] = {
    var commonVars: Vector[String] = Vector.empty
    varsFirst foreach {
      variable => {
        if (varsSecond.contains(variable)) {
          commonVars = commonVars :+ variable
        }
      }
    }
    commonVars
  }

  /**
   * Do some optimizations about empty sets
   * @param resultSet
   * @param commonVars
   * @return
   */
  def generateBucketMap(resultSet: ResultSet, commonVars: Vector[String]): HashMap[Int, Vector[Binding]] = {
    var bucketMap: HashMap[Int, Vector[Binding]] = HashMap.empty

    for (i <- 0 until ParallelJoinManager.bucketSize) {
      bucketMap += (i -> Vector.empty[Binding])
    }

    while (resultSet.hasNext) {
      val binding = resultSet.nextBinding
      val multipleNode = getMultipleNode(commonVars, binding)
      val index = findIndex(multipleNode)
      val bindings = bucketMap(index)
      val newBindings = bindings :+ binding
      bucketMap += (index -> newBindings)
    }

    bucketMap
  }

  def getMultipleNode(commonVars: Vector[String], binding: Binding): MultipleNode = {
    val multipleNode = new MultipleNode
    for (commonVar <- commonVars) {
      multipleNode.add(binding.get(Var.alloc(commonVar)))
    }
    multipleNode
  }

  private def findIndex(multipleNode: MultipleNode) = {
    var index = multipleNode.hashCode % ParallelJoinManager.bucketSize
    if (index < 0) index += ParallelJoinManager.bucketSize
    index
  }

  private def insertResult(resultSet: ResultSet): Unit = {
    while (resultSet.hasNext) {
      bindings = bindings :+ resultSet.nextBinding
    }
  }

  private def registerSender = {
    if (!registeryList.contains(sender)) {
      registeryList = registeryList :+ sender
    }
  }

  private def notifyRegisteryList(result: Result) = {
    registeryList foreach {
      registered => {
        registered ! result
      }
    }
  }

}