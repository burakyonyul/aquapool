package querying.actor.wrapper

import akka.actor.{Actor, ActorLogging, Props}
import org.apache.spark.util.SizeEstimator
import querying.main.MonitoringUtils
import querying.message.ExecuteQuery

object PostgresqlExecutor{
  def props: Props = Props(new PostgresqlExecutor)
}

class PostgresqlExecutor extends Actor with ActorLogging {

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
      sender ! result
      val sizeInBytes = SizeEstimator.estimate(result)
      log.info("Size of the new result message sent start RdfStoreExecutor end Distributor is: [{}] Bytes, and is [{}]", sizeInBytes, MonitoringUtils.formatByteValue(sizeInBytes))
  }

  protected def executeQuery(query: String) = {

    //TODO: implement
    None
  }

}