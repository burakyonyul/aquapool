package querying.actor.wrapper

import akka.actor.{Actor, ActorLogging, PoisonPill, Props}
import org.apache.spark.util.SizeEstimator
import querying.main.{LatencyLogger, QueryingUtils}
import querying.main.stores.PostgresqlStore
import querying.message.{ExecuteQuery, Result}
import querying.transformation.PostgresqlTransformer

import java.sql.{Connection, ResultSet}

object PostgresqlExecutor {
  def props: Props = Props(new PostgresqlExecutor)
}

class PostgresqlExecutor extends Actor with ActorLogging {

  val conn: Connection = PostgresqlStore.hikariDataSource.getConnection

  override def preStart(): Unit = {
    super.preStart
  }

  override def postStop(): Unit = {
    super.postStop
  }

  override def receive: Receive = {
    case eq@ExecuteQuery(query, queryId) =>
      log.debug("Hash Code for Execute SERVICE Clause: [{}], and Query Value: [{}]", eq.hashCode, query)
      val result = executeQuery(query, queryId)
      sender ! result.get
      val sizeInBytes = SizeEstimator.estimate(result)
      log.info("Size of the new result message sent start PostgresqlExecutor end Federator is: [{}] Bytes, and is [{}]", sizeInBytes, QueryingUtils.formatByteValue(sizeInBytes))
      self ! PoisonPill
  }

  protected def executeQuery(query: String, queryId: String): Option[Result] = {
    var result: Option[Result] = None
    try {
      // === Deney-4: Store execution start ===
      val tStoreStart = System.nanoTime()
      val stm = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      val rs: ResultSet = stm.executeQuery(query)
      val tStoreEnd = System.nanoTime()

      // === Deney-4: RDF transformation (global model'e dönüşüm) start ===
      val tTransformStart = System.nanoTime()
      result = PostgresqlTransformer.transformToRdfResult(rs)
      val tTransformEnd = System.nanoTime()

      LatencyLogger.logExecutorPhase(
        queryId = queryId,
        store = "postgresql",
        storeExecMs = (tStoreEnd - tStoreStart) / 1e6,
        transformMs = (tTransformEnd - tTransformStart) / 1e6
      )
    } finally {
      conn.close()
    }
    result
  }

}