package querying.actor.wrapper

import akka.actor.{Actor, ActorLogging, ActorSystem, PoisonPill, Props}
import akka.stream.scaladsl.Sink
import com.influxdb.query.FluxRecord
import org.apache.spark.util.SizeEstimator
import querying.main.stores.InfluxdbStore
import querying.main.{LatencyLogger, QueryingUtils}
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
    case eq@ExecuteQuery(query, queryId) =>
      log.debug("Hash Code for Execute SERVICE Clause: [{}], and Query Value: [{}]", eq.hashCode, query)
      val result = executeQuery(query, queryId)
      sender ! result.get
      val sizeInBytes = SizeEstimator.estimate(result)
      log.info("Size of the new result message sent start InfluxdbExecutor end Federator is: [{}] Bytes, and is [{}]", sizeInBytes, QueryingUtils.formatByteValue(sizeInBytes))
      self ! PoisonPill
  }

  protected def executeQuery(query: String, queryId: String): Option[Result] = {

    val client = InfluxdbStore.getClient()
    val transformer = new InfluxdbTransformer()

    // === Experiment-4: Store execution start ===
    // (InfluxDB stream-based: generateMeasurementResource inside Sink.foreach
    // adds to the RDF model for each record; the distinction between pure "DB I/O" and pure "model
    // construction" cannot be made due to the stream API → both are included in the store
    // phase. transform_ms is the SPARQL evolution time.)
    val tStoreStart = System.nanoTime()
    val influxResults = client.getQueryScalaApi().query(query)
    val sink = influxResults
      .runWith(Sink.foreach[FluxRecord](
        fluxRecord => transformer.generateMeasurementResource(fluxRecord)
      ))
    Await.result(sink, Duration.Inf)
    client.close()
    val tStoreEnd = System.nanoTime()

    // === Experiment-4: RDF transformation (final SPARQL projection) start ===
    val tTransformStart = System.nanoTime()
    val result = transformer.transformToRdfResult()
    val tTransformEnd = System.nanoTime()

    LatencyLogger.logExecutorPhase(
      queryId = queryId,
      store = "influxdb",
      storeExecMs = (tStoreEnd - tStoreStart) / 1e6,
      transformMs = (tTransformEnd - tTransformStart) / 1e6
    )
    result
  }

}