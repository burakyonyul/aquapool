package test

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import com.influxdb.query.FluxRecord
import com.typesafe.config.Config
import querying.main.stores.InfluxdbStore
import querying.message.Result
import querying.transformation.InfluxdbTransformer

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class DummyInfluxdbExecutor {

  import com.typesafe.config.ConfigFactory

  val myConfig: Config = ConfigFactory.parseString("something=somethingElse")
  implicit val system: ActorSystem = ActorSystem("dummy-influxdb-tranformator", config = myConfig)

  def executeQuery(query: String): Option[Result] = {

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
    system.terminate()
    transformer.transformToRdfResult()
  }

}
