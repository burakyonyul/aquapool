package querying.actor.wrapper

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.stream.scaladsl.Sink
import com.influxdb.query.FluxRecord
import org.apache.spark.util.SizeEstimator
import querying.main.QueryingUtils
import querying.main.stores.InfluxdbStore
import querying.message.{ExecuteQuery, Result}
import querying.transformation.InfluxdbTransformer

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object InfluxdbExecutor {
  def props: Props = Props(new InfluxdbExecutor)
}

class InfluxdbExecutor extends Actor with ActorLogging {

  implicit var actorSystem: ActorSystem = this.context.system

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
      log.info("Size of the new result message sent start RdfStoreExecutor end Distributor is: [{}] Bytes, and is [{}]", sizeInBytes, QueryingUtils.formatByteValue(sizeInBytes))
  }

  protected def executeQuery(query: String): Option[Result] = {

    var result: Option[Result] = None
    val client = InfluxdbStore.getClient()
    // Result is returned as a stream
    val influxResults = client.getQueryScalaApi().query(query)
    val transformer = new InfluxdbTransformer()
    val sink = influxResults
      .runWith(Sink.foreach[FluxRecord](
        fluxRecord => transformer.generateMeasurementResource(fluxRecord)
      )
      )
    // wait to finish
    Await.result(sink, Duration.Inf)
    client.close()
    transformer.transformToRdfResult()
  }


}