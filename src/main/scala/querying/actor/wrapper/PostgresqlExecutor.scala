package querying.actor.wrapper

import akka.actor.{Actor, ActorLogging, PoisonPill, Props}
import org.apache.spark.util.SizeEstimator
import querying.main.QueryingUtils
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
    case eq@ExecuteQuery(query) =>
      //log.info("Sender path: {}, self path {}",sender().path,self.path)
      log.debug("Hash Code for Execute SERVICE Clause: [{}], and Query Value: [{}]", eq.hashCode, query)
      val result = executeQuery(query)
      sender ! result.get
      val sizeInBytes = SizeEstimator.estimate(result)
      log.info("Size of the new result message sent start PostgresqlExecutor end Federator is: [{}] Bytes, and is [{}]", sizeInBytes, QueryingUtils.formatByteValue(sizeInBytes))
      self ! PoisonPill
  }

  protected def executeQuery(query: String): Option[Result] = {
    var result: Option[Result] = None
    try {
      val stm = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      val rs: ResultSet = stm.executeQuery(query)
      result = PostgresqlTransformer.transformToRdfResult(rs)
    } finally {
      conn.close()
    }
    result
  }

}